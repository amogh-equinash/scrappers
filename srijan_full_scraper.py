"""
srijan_full_scraper.py — Entity-Centric Production Srijan Crawler
==================================================================

ARCHITECTURE OVERVIEW
---------------------
This crawler replaces the page-centric model with an entity-centric model.

WHY THE OLD APPROACH FAILED
  1. Page-number resume: The site reshuffles product order on every session
     start (page.goto). Page 47 on resume contains different products than
     page 47 in the original run → duplicates and gaps.
  2. Stop on "zero new products this page": Because pages reshuffle, a page
     can contain 100 products already seen from earlier pages in THIS run.
     This is not the same as "no more products exist." The old scraper
     interpreted it as "done" and stopped at page 47/48.
  3. ViewState accumulation: Each Playwright postback grew ViewState. After
     ~46 pages (~9,200 postbacks) the state became too large to process.

THE NEW MODEL: ENTITY-FIRST, PAGE-AGNOSTIC
  • State is tracked per entity (product_id), not per page.
  • SQLite is the truth store. Seen product_ids are loaded on every startup.
  • Discovery and extraction are interleaved per page visit:
      1. Playwright loads a listing page (1 postback/page).
      2. ALL card product_ids are read from the DOM WITHOUT clicking.
      3. Only cards with UNSEEN product_ids are sent for detail extraction.
      4. httpx fires those detail requests concurrently (15 at a time).
      5. Results are immediately stored in SQLite and flushed to JSON.
  • After one full pass through all pages, a new cycle starts with the same
    pages (now reshuffled). Each cycle surfaces previously unseen products.
  • Stop conditions (NOT "zero new this page"):
      - extracted_count >= expected_total * 98.5%
      - OR: N consecutive full cycles all produce zero new products

ARCHITECTURE COMPONENTS
  SrijanDB       — SQLite state: entity store + page visit log + crawl state
  CrawlEngine    — Orchestrates discovery+extraction cycles via async Playwright
  DetailFetcher  — Concurrent httpx detail requests using per-page session
  EntityExtractor— BeautifulSoup HTML parsing of UpdatePanel delta responses
  OutputWriter   — Writes JSON chunks to srijan-scrapped/

CRAWLEE DESIGN PRINCIPLES APPLIED
  Even without using Crawlee's URL-based crawler (which cannot model ASP.NET
  postback navigation), this implementation follows Crawlee's architecture:
  • Persistent request queue     → SQLite page_visits table
  • Entity-level deduplication   → seen_ids set backed by SQLite
  • Crash-safe resume            → crawl_state table + resume_page key
  • Retry logic                  → per-request retry with exponential backoff
  • Concurrency control          → asyncio.Semaphore(15) over httpx pool
  • Browser lifecycle separation → Playwright only for session + pagination

OUTPUT
  srijan-scrapped/crawl_state.db         — SQLite truth store (survives crashes)
  srijan-scrapped/products_NNNNN_MMMMM.json — JSON chunks (IC pipeline input)

RESUME
  Run: python3 srijan_full_scraper.py
  Attach to existing Chrome:  set CDP_PORT, leave browser open after Ctrl+C.
  On restart: reads SQLite, loads seen_ids, resumes from saved resume_page.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import sqlite3
import time
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import quote, unquote, urlparse, urlunparse

import httpx
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, Browser, BrowserContext, Page

# ── Runtime constants ─────────────────────────────────────────────────────────

TARGET       = "https://srijandefence.gov.in/ProductMarketPlace"
OUTPUT_DIR   = "srijan-scrapped"
DB_PATH      = os.path.join(OUTPUT_DIR, "crawl_state.db")
CHUNK_SIZE   = 500      # products per output JSON file
CONCURRENCY  = 15       # max concurrent httpx detail requests per page
MAX_PAGES    = 490      # upper bound; real count read from page UI
STOP_CYCLES  = 5        # consecutive zero-new cycles before stopping
MIN_COVERAGE = 0.985    # stop when 98.5% of expected total is extracted
CDP_PORT     = 9224     # Chrome remote debugging port (for resume attach)
PAGE_DELAY   = 1.5      # seconds between page advances (politeness)

# ── Field → element-ID map ────────────────────────────────────────────────────

FIELD_IDS: dict[str, str] = {
    "dpsu_shq":                    "ContentPlaceHolder1_lblcompname",
    "division":                    "ContentPlaceHolder1_lbldiviname",
    "unit":                        "ContentPlaceHolder1_lblunitnamepro",
    "product_id":                  "ContentPlaceHolder1_lblrefnoview",
    "product_name":                "ContentPlaceHolder1_lblitemname1",
    "dpsu_part_number":            "ContentPlaceHolder1_lbldpsupartno",
    "hsn_code":                    "ContentPlaceHolder1_lblhsncode8digit",
    "industry_domain":             "ContentPlaceHolder1_prodIndustryDomain",
    "industry_subdomain":          "ContentPlaceHolder1_ProdIndusSubDomain",
    "oem_name_country":            "ContentPlaceHolder1_lbloemname",
    "oem_part_number":             "ContentPlaceHolder1_lbloempartno",
    "nato_supply_group":           "ContentPlaceHolder1_lblnsngroup",
    "nato_supply_class":           "ContentPlaceHolder1_lblnsngroupclass",
    "item_name_code":              "ContentPlaceHolder1_lblclassitem",
    "nsc_code_4digit":             "ContentPlaceHolder1_lblnsccode4digit",
    "spec_item_name":              "ContentPlaceHolder1_itemname2",
    "feature_details":             "ContentPlaceHolder1_lblfeature",
    "quality_assurance":           "ContentPlaceHolder1_lbqa",
    "import_value_lakh":           "ContentPlaceHolder1_lblvalueimport",
    "indigenization_target_year":  "ContentPlaceHolder1_lblindtrgyr",
    "indigenization_started":      "ContentPlaceHolder1_lblindstart",
    "make_in_india_category":      "ContentPlaceHolder1_lblindicate",
    "eoi_rfp":                     "ContentPlaceHolder1_lbleoirep",
    "indigenized_by":              "ContentPlaceHolder1_lblindigby",
    "contact_name":                "ContentPlaceHolder1_lblempname",
    "contact_designation":         "ContentPlaceHolder1_lbldesignation",
    "contact_email":               "ContentPlaceHolder1_lblemailidpro",
    "contact_phone":               "ContentPlaceHolder1_lblphonenumber",
}


# ════════════════════════════════════════════════════════════════════════════
# PERSISTENCE LAYER — SQLite entity store
# ════════════════════════════════════════════════════════════════════════════

class SrijanDB:
    """
    SQLite-backed entity store.  Three tables:

    products     — one row per scraped entity (product_id is PK)
    page_visits  — audit log of every page visit (page_num, cycle, stats)
    crawl_state  — key/value store for resume pointers

    All writes use WAL mode for crash safety.  The in-memory seen_ids set is
    the hot dedup index; it is rebuilt from the DB on every startup.
    """

    _SCHEMA = """
    CREATE TABLE IF NOT EXISTS products (
        product_id                TEXT PRIMARY KEY,
        dpsu_shq                  TEXT DEFAULT '',
        division                  TEXT DEFAULT '',
        unit                      TEXT DEFAULT '',
        product_name              TEXT DEFAULT '',
        dpsu_part_number          TEXT DEFAULT '',
        hsn_code                  TEXT DEFAULT '',
        industry_domain           TEXT DEFAULT '',
        industry_subdomain        TEXT DEFAULT '',
        oem_name_country          TEXT DEFAULT '',
        oem_part_number           TEXT DEFAULT '',
        nato_supply_group         TEXT DEFAULT '',
        nato_supply_class         TEXT DEFAULT '',
        item_name_code            TEXT DEFAULT '',
        nsc_code_4digit           TEXT DEFAULT '',
        spec_item_name            TEXT DEFAULT '',
        feature_details           TEXT DEFAULT '',
        quality_assurance         TEXT DEFAULT '',
        import_value_lakh         TEXT DEFAULT '',
        indigenization_target_year TEXT DEFAULT '',
        indigenization_started    TEXT DEFAULT '',
        make_in_india_category    TEXT DEFAULT '',
        eoi_rfp                   TEXT DEFAULT '',
        indigenized_by            TEXT DEFAULT '',
        contact_name              TEXT DEFAULT '',
        contact_designation       TEXT DEFAULT '',
        contact_email             TEXT DEFAULT '',
        contact_phone             TEXT DEFAULT '',
        import_history_past       TEXT DEFAULT '',
        import_history_projected  TEXT DEFAULT '',
        item_specification        TEXT DEFAULT '',
        product_image_urls        TEXT DEFAULT '[]',
        document_urls             TEXT DEFAULT '[]',
        status                    TEXT DEFAULT 'extracted',
        retry_count               INTEGER DEFAULT 0,
        scraped_at                TEXT,
        source_page               INTEGER,
        fetch_method              TEXT DEFAULT 'httpx'
    );

    CREATE TABLE IF NOT EXISTS page_visits (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        page_num     INTEGER NOT NULL,
        cycle        INTEGER NOT NULL,
        visited_at   TEXT NOT NULL,
        cards_found  INTEGER DEFAULT 0,
        new_products INTEGER DEFAULT 0,
        httpx_ok     INTEGER DEFAULT 0,
        httpx_fail   INTEGER DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS crawl_state (
        key   TEXT PRIMARY KEY,
        value TEXT NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_pv_page  ON page_visits(page_num, cycle);
    CREATE INDEX IF NOT EXISTS idx_prod_status ON products(status);
    """

    # 37 positional bind vars for INSERT OR REPLACE into products
    _INSERT_SQL = """
        INSERT OR REPLACE INTO products (
            product_id, dpsu_shq, division, unit, product_name, dpsu_part_number,
            hsn_code, industry_domain, industry_subdomain, oem_name_country,
            oem_part_number, nato_supply_group, nato_supply_class, item_name_code,
            nsc_code_4digit, spec_item_name, feature_details, quality_assurance,
            import_value_lakh, indigenization_target_year, indigenization_started,
            make_in_india_category, eoi_rfp, indigenized_by, contact_name,
            contact_designation, contact_email, contact_phone,
            import_history_past, import_history_projected, item_specification,
            product_image_urls, document_urls,
            status, scraped_at, source_page, fetch_method
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """

    def __init__(self, db_path: str) -> None:
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.executescript(self._SCHEMA)
        self._conn.commit()
        self.seen_ids: set[str] = self._load_seen_ids()

    def _load_seen_ids(self) -> set[str]:
        cur = self._conn.execute("SELECT product_id FROM products WHERE status='extracted'")
        ids = {row[0] for row in cur.fetchall()}
        print(f"  [DB] Loaded {len(ids):,} extracted product IDs.")
        return ids

    # ── Entity checks ──────────────────────────────────────────────────────────

    def is_seen(self, product_id: str) -> bool:
        return bool(product_id) and product_id in self.seen_ids

    def count(self) -> int:
        return len(self.seen_ids)

    # ── Writes ─────────────────────────────────────────────────────────────────

    def insert_product(self, product: dict) -> bool:
        """Insert product into DB. Returns True if this was a new (unseen) product."""
        pid = (product.get("product_id") or "").strip()
        if not pid:
            return False
        if pid in self.seen_ids:
            return False

        now = datetime.now(timezone.utc).isoformat()
        imgs = json.dumps(product.get("product_image_urls") or [])
        docs = json.dumps(product.get("document_urls") or [])

        self._conn.execute(self._INSERT_SQL, (
            pid,
            product.get("dpsu_shq", ""),
            product.get("division", ""),
            product.get("unit", ""),
            product.get("product_name", ""),
            product.get("dpsu_part_number", ""),
            product.get("hsn_code", ""),
            product.get("industry_domain", ""),
            product.get("industry_subdomain", ""),
            product.get("oem_name_country", ""),
            product.get("oem_part_number", ""),
            product.get("nato_supply_group", ""),
            product.get("nato_supply_class", ""),
            product.get("item_name_code", ""),
            product.get("nsc_code_4digit", ""),
            product.get("spec_item_name", ""),
            product.get("feature_details", ""),
            product.get("quality_assurance", ""),
            product.get("import_value_lakh", ""),
            product.get("indigenization_target_year", ""),
            product.get("indigenization_started", ""),
            product.get("make_in_india_category", ""),
            product.get("eoi_rfp", ""),
            product.get("indigenized_by", ""),
            product.get("contact_name", ""),
            product.get("contact_designation", ""),
            product.get("contact_email", ""),
            product.get("contact_phone", ""),
            product.get("import_history_past", ""),
            product.get("import_history_projected", ""),
            product.get("item_specification", ""),
            imgs,
            docs,
            "extracted",
            now,
            product.get("_source_page"),
            product.get("_fetch_method", "httpx"),
        ))
        self._conn.commit()
        self.seen_ids.add(pid)
        return True

    def log_page_visit(
        self, page_num: int, cycle: int,
        cards: int, new: int, ok: int, fail: int
    ) -> None:
        self._conn.execute(
            "INSERT INTO page_visits "
            "(page_num, cycle, visited_at, cards_found, new_products, httpx_ok, httpx_fail) "
            "VALUES (?,?,?,?,?,?,?)",
            (page_num, cycle, datetime.now(timezone.utc).isoformat(), cards, new, ok, fail),
        )
        self._conn.commit()

    # ── Crawl state key/value ──────────────────────────────────────────────────

    def get_state(self, key: str, default: str = "") -> str:
        cur = self._conn.execute("SELECT value FROM crawl_state WHERE key=?", (key,))
        row = cur.fetchone()
        return row[0] if row else default

    def set_state(self, key: str, value) -> None:
        self._conn.execute(
            "INSERT OR REPLACE INTO crawl_state (key, value) VALUES (?,?)",
            (key, str(value)),
        )
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()


# ════════════════════════════════════════════════════════════════════════════
# OUTPUT WRITER — JSON chunks
# ════════════════════════════════════════════════════════════════════════════

class OutputWriter:
    """
    Buffers extracted products and flushes them to numbered JSON chunks.
    Chunk naming: products_NNNNN_MMMMM.json (1-indexed, zero-padded to 5).
    Existing chunks are counted on startup so numbering stays correct on resume.
    """

    def __init__(self, output_dir: str, chunk_size: int = CHUNK_SIZE) -> None:
        os.makedirs(output_dir, exist_ok=True)
        self._dir       = output_dir
        self._chunk     = chunk_size
        self._buf: list[dict] = []
        self._written   = self._count_existing()
        print(f"  [Writer] {self._written:,} products already in output files.")

    def _count_existing(self) -> int:
        total = 0
        for fname in sorted(os.listdir(self._dir)):
            if re.match(r"products_\d+_\d+\.json", fname):
                try:
                    with open(os.path.join(self._dir, fname), encoding="utf-8") as f:
                        total += len(json.load(f))
                except Exception:
                    pass
        return total

    def add(self, products: list[dict]) -> None:
        self._buf.extend(products)
        while len(self._buf) >= self._chunk:
            self._flush_chunk(self._chunk)

    def flush(self) -> None:
        if self._buf:
            self._flush_chunk(len(self._buf))

    def _flush_chunk(self, n: int) -> None:
        chunk = self._buf[:n]
        self._buf = self._buf[n:]
        start = self._written + 1
        end   = self._written + len(chunk)
        fname = os.path.join(self._dir, f"products_{start:05d}_{end:05d}.json")
        output = []
        for p in chunk:
            row = {k: v for k, v in p.items() if not k.startswith("_")}
            # Deserialise if DB gave us JSON strings
            for key in ("product_image_urls", "document_urls"):
                if isinstance(row.get(key), str):
                    try:
                        row[key] = json.loads(row[key])
                    except Exception:
                        row[key] = []
            output.append(row)
        with open(fname, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        self._written += len(chunk)
        print(f"  [SAVED] {fname}  ({len(chunk)} products, total {self._written:,})")


# ════════════════════════════════════════════════════════════════════════════
# HTML / SESSION EXTRACTION
# ════════════════════════════════════════════════════════════════════════════

def _parse_table(soup: BeautifulSoup, table_id: str) -> str:
    tbl = soup.find(id=table_id)
    rows: list[str] = []
    if tbl:
        for tr in tbl.find_all("tr")[1:]:
            cells = tr.find_all("td")
            if len(cells) >= 4:
                rows.append(
                    f"{cells[0].get_text(strip=True)}"
                    f" | qty:{cells[1].get_text(strip=True)}"
                    f" | {cells[2].get_text(strip=True)}"
                    f" | Rs.{cells[3].get_text(strip=True)}L"
                )
    return "; ".join(rows)


def extract_product_from_html(html: str) -> dict:
    """Parse UpdatePanel delta HTML → product dict."""
    soup = BeautifulSoup(html, "html.parser")
    product: dict = {}

    for field, el_id in FIELD_IDS.items():
        el = soup.find(id=el_id)
        product[field] = el.get_text(strip=True) if el else ""

    product["import_history_past"]      = _parse_table(soup, "ContentPlaceHolder1_gvestimatequanold")
    product["import_history_projected"] = _parse_table(soup, "ContentPlaceHolder1_gvestimatequanorprice")

    parts: list[str] = []
    if product.get("spec_item_name"):   parts.append("Item: "    + product["spec_item_name"])
    if product.get("feature_details"): parts.append("Feature: " + product["feature_details"])
    if product.get("quality_assurance"): parts.append("QA: "    + product["quality_assurance"])
    product["item_specification"] = " | ".join(parts)

    BASE  = "https://srijandefence.gov.in"

    def _abs_url(raw: str) -> str:
        """Make URL absolute and percent-encode any unencoded characters (spaces etc.)."""
        if not raw:
            return ""
        url = raw if raw.startswith("http") else f"{BASE}/{raw.lstrip('/')}"
        p = urlparse(url)
        # unquote→requote normalises mixed-encoded paths (e.g. raw spaces alongside %20)
        encoded_path = quote(unquote(p.path), safe="/%")
        return urlunparse(p._replace(path=encoded_path))

    modal = soup.find(id="ProductCompany")
    imgs: list[str] = []
    if modal:
        for img in modal.find_all("img", id=re.compile(r"dlimage.*imgprodimage")):
            src = img.get("src", "")
            if src:
                imgs.append(_abs_url(src))
        if not imgs:
            skip = ("logo", "icon", "spacer", "blank", "arrow", "bullet")
            for img in modal.find_all("img"):
                src = img.get("src", "")
                if src and not any(s in src.lower() for s in skip):
                    imgs.append(_abs_url(src))
    product["product_image_urls"] = imgs

    docs: list[str] = []
    if modal:
        for a in modal.find_all("a", href=True):
            href = a["href"]
            if re.search(r"\.pdf(\?|$)", href, re.I):
                docs.append(_abs_url(href))
        if not docs:
            for el in modal.find_all(["td", "span", "a", "div"]):
                m = re.search(r"([\w.\- ]+\.pdf)", el.get_text(strip=True), re.I)
                if m:
                    docs.append(_abs_url(f"Upload/{m.group(1).strip()}"))
    product["document_urls"] = list(dict.fromkeys(docs))

    return product


def parse_updatepanel_response(text: str) -> Optional[str]:
    """
    Parse ASP.NET UpdatePanel delta wire format.
    Format: length|type|id|content| repeated.
    Returns the HTML content of the first 'updatePanel' segment.
    """
    if not text:
        return None
    try:
        pos = 0
        n   = len(text)
        while pos < n:
            p1 = text.index("|", pos)
            seg_len_str = text[pos:p1]
            if not seg_len_str.isdigit():
                break
            seg_len = int(seg_len_str)
            pos = p1 + 1
            p2 = text.index("|", pos);  seg_type = text[pos:p2]; pos = p2 + 1
            p3 = text.index("|", pos);  pos = p3 + 1
            content = text[pos : pos + seg_len]
            pos += seg_len + 1
            if seg_type == "updatePanel":
                return content
    except (ValueError, IndexError):
        pass
    return None


# ════════════════════════════════════════════════════════════════════════════
# BROWSER / SESSION HELPERS
# ════════════════════════════════════════════════════════════════════════════

async def extract_page_session(page: Page) -> dict:
    """
    Snapshot ALL form fields via the browser's FormData API.
    This is the only correct approach for ASP.NET WebForms pages:
    sending only hidden inputs misses select boxes and text inputs that
    are part of the EventValidation token computation.

    ScriptManager ID note: ToolkitScriptManager does NOT write a hidden input
    to the DOM — it adds its field dynamically via JavaScript before each XHR.
    We read it from the PageRequestManager JS object, not from the DOM.
    Wrong key → server returns a ViewState-update response with no updatePanel
    segment (silent failure, all httpx requests appear to return 200 but parse
    fails because there is no updatePanel segment in the delta response).
    """
    extra_fields: dict = await page.evaluate("""
        () => {
            const form = document.querySelector('form');
            if (!form) return {};
            const r = {};
            for (const [k, v] of new FormData(form).entries())
                if (typeof v === 'string') r[k] = v;
            return r;
        }
    """)
    # Read ScriptManager UniqueID from PageRequestManager JS object.
    # UniqueID uses '$' separators (correct for POST field names).
    sm_id: str = await page.evaluate("""
        () => {
            try {
                const pm = Sys.WebForms.PageRequestManager.getInstance();
                if (pm && pm._scriptManager) {
                    const uid = pm._scriptManager.get_uniqueID();
                    if (uid && uid.length > 3) return uid;
                }
                // _scriptManagerID is the client ID (underscores); convert to UniqueID (dollars)
                if (pm && pm._scriptManagerID) {
                    return pm._scriptManagerID.replace(/_/g, '$');
                }
            } catch(e) {}
            // DOM fallback: ToolkitScriptManager element (may not exist as hidden input)
            const t = document.querySelector('[id*="ToolkitScriptManager"]');
            if (t && t.id) return t.id.replace(/_/g, '$');
            const s = document.querySelector('input[id*="ScriptManager"][type="hidden"]');
            if (s && s.name) return s.name;
            return 'ctl00$ContentPlaceHolder1$ToolkitScriptManager1';
        }
    """)
    cookies = {c["name"]: c["value"] for c in await page.context.cookies()}
    return {
        "viewstate":         extra_fields.get("__VIEWSTATE", ""),
        "script_manager_id": sm_id,
        "cookies":           cookies,
        "extra_fields":      extra_fields,
    }


async def extract_listing_cards(page: Page) -> list[dict]:
    """
    Extract stable card identifiers from the listing page WITHOUT clicking.

    For each card returns:
      product_id   — from hidden input[id*='hfr'] (stable PRO###### identifier)
      event_target — __doPostBack target for the detail postback
      card_index   — 0-based position in current page listing

    This is the core of entity-centric design: we know WHICH products are on
    this page before we fetch any detail, enabling precise deduplication.
    """
    return await page.evaluate("""
        () => {
            const results = [];
            document.querySelectorAll("a[id*='LinkButton13']").forEach((a, idx) => {
                const m = (a.getAttribute('href') || '').match(/__doPostBack\\('([^']+)'/);
                if (!m) return;
                const target = m[1];

                // Walk up to the card container (div.product-item, tr, or parent)
                const container = a.closest('div.product-item')
                               || a.closest('tr')
                               || a.closest('li')
                               || a.parentElement?.parentElement;

                let pid = null;
                if (container) {
                    // Primary: hidden input with product reference
                    const hfr = container.querySelector('input[id*="hfr"]')
                             || container.querySelector('input[id*="HiddenRef"]');
                    if (hfr && hfr.value) pid = hfr.value.trim();

                    // Fallback: any visible reference label
                    if (!pid) {
                        const ref = container.querySelector('[id*="lblref"],[id*="lblRef"]');
                        if (ref && ref.textContent) pid = ref.textContent.trim() || null;
                    }
                }

                results.push({ product_id: pid, event_target: target, card_index: idx });
            });
            return results;
        }
    """)


async def get_site_totals(page: Page) -> tuple[Optional[int], Optional[int]]:
    """
    Read total product count and page count from the page UI.
    Returns (total_products, total_pages) — either may be None if not found.
    """
    raw = await page.evaluate("""
        () => {
            const find = (...sels) => {
                for (const s of sels) {
                    const el = document.querySelector(s);
                    if (el && el.innerText.trim()) return el.innerText.trim();
                }
                return null;
            };
            return {
                total:  find('#ContentPlaceHolder1_lbltotalrec',
                             '[id*="lbltotalrec"]', '[id*="lblTotal"]',
                             '[id*="lblcount"]',   '[id*="lbltotal"]'),
                pages:  find('#ContentPlaceHolder1_lbltotalpages',
                             '[id*="lbltotalpages"]', '[id*="TotalPage"]',
                             '[id*="totalpage"]'),
            };
        }
    """)

    def parse_int(s: Optional[str]) -> Optional[int]:
        if not s:
            return None
        m = re.search(r"[\d,]+", s)
        return int(m.group().replace(",", "")) if m else None

    return parse_int(raw.get("total")), parse_int(raw.get("pages"))


# ════════════════════════════════════════════════════════════════════════════
# PAGINATION
# ════════════════════════════════════════════════════════════════════════════

async def _fingerprint(page: Page) -> str:
    """Short fingerprint of current listing content (first 8 card titles)."""
    return await page.evaluate("""
        () => Array.from(document.querySelectorAll("a[id*='LinkButton13']"))
               .slice(0, 8)
               .map(a => (a.closest('div.product-item,tr,li')?.innerText || '').trim().slice(0, 35))
               .join('||')
    """)


async def jump_to_page(page: Page, target: int, wait_secs: int = 18) -> bool:
    """
    Navigate the ASP.NET listing to an arbitrary page number via the jump input.
    Returns True on success.
    """
    before = await _fingerprint(page)
    await page.fill("#ContentPlaceHolder1_txtindex", str(target))
    try:
        async with page.expect_response(
            lambda r: "ProductMarketPlace" in r.url and r.request.method == "POST",
            timeout=22000,
        ):
            await page.click("#ContentPlaceHolder1_btnGo")
    except Exception as exc:
        print(f"  [WARN] jump_to_page {target}: {exc}")

    for _ in range(wait_secs):
        await asyncio.sleep(1.0)
        if await _fingerprint(page) != before:
            await page.wait_for_selector("a[id*='LinkButton13']", timeout=12000)
            await asyncio.sleep(0.6)
            return True

    # Confirm via index field value
    current = await page.evaluate(
        "() => document.getElementById('ContentPlaceHolder1_txtindex')?.value || ''"
    )
    return str(current).strip() == str(target)


async def go_next_page(page: Page, wait_secs: int = 14) -> bool:
    """
    Advance one page via the Next button postback.
    Returns True on success.
    """
    BTN_ID   = "ContentPlaceHolder1_lnkbtnPgNext"
    POSTBACK = "ctl00$ContentPlaceHolder1$lnkbtnPgNext"

    if not await page.evaluate(f"() => !!document.getElementById('{BTN_ID}')"):
        return False

    before = await _fingerprint(page)
    for attempt in range(1, 3):
        try:
            async with page.expect_response(
                lambda r: "ProductMarketPlace" in r.url and r.request.method == "POST",
                timeout=22000,
            ):
                await page.evaluate(f"__doPostBack('{POSTBACK}', '')")
        except Exception as exc:
            print(f"  [WARN] go_next_page attempt {attempt}: {exc}")

        for _ in range(wait_secs):
            await asyncio.sleep(1.0)
            if await _fingerprint(page) != before:
                await page.wait_for_selector("a[id*='LinkButton13']", timeout=12000)
                await asyncio.sleep(0.6)
                return True

    return False


# ════════════════════════════════════════════════════════════════════════════
# DETAIL FETCHER — httpx concurrent
# ════════════════════════════════════════════════════════════════════════════

async def _fetch_one_card(
    session: dict,
    card: dict,
    page_num: int,
    sem: asyncio.Semaphore,
    client: httpx.AsyncClient,
) -> Optional[dict]:
    """
    Fetch detail for one card via httpx UpdatePanel postback.
    Uses exponential backoff on failure (3 attempts).
    Returns parsed product dict or None on permanent failure.
    """
    sm_id     = session["script_manager_id"]
    post_data = dict(session["extra_fields"])
    post_data.update({
        "__EVENTTARGET":   card["event_target"],
        "__EVENTARGUMENT": "",
        "__ASYNCPOST":     "true",
        sm_id:             f"ctl00$ContentPlaceHolder1$update|{card['event_target']}",
    })
    headers = {
        "X-MicrosoftAjax":  "Delta=true",
        "X-Requested-With": "XMLHttpRequest",
        "Content-Type":     "application/x-www-form-urlencoded; charset=UTF-8",
        "Referer":          TARGET,
        "Origin":           "https://srijandefence.gov.in",
        "User-Agent":       (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
    }

    async with sem:
        for attempt in range(1, 4):
            try:
                resp = await client.post(
                    TARGET, data=post_data, headers=headers,
                    cookies=session["cookies"], timeout=35.0,
                )
                resp.raise_for_status()
                break
            except Exception as exc:
                if attempt == 3:
                    short = card["event_target"].split("$")[-2] if "$" in card["event_target"] else "?"
                    print(f"    [FAIL] card {short}: {exc}")
                    return None
                await asyncio.sleep(attempt * 2.5)
        else:
            return None

    html = parse_updatepanel_response(resp.text)
    if html is None:
        return None

    product = extract_product_from_html(html)

    # If listing gave us a product_id and detail didn't, use listing's
    if not product.get("product_id") and card.get("product_id"):
        product["product_id"] = card["product_id"]

    product["_source_page"] = page_num
    product["_fetch_method"] = "httpx"
    return product


async def fetch_cards_batch(
    session: dict,
    cards: list[dict],
    page_num: int,
    sem: asyncio.Semaphore,
    client: httpx.AsyncClient,
) -> tuple[list[dict], int, int]:
    """
    Concurrently fetch detail for all cards in the list.
    Returns (products, ok_count, fail_count).
    """
    tasks = [_fetch_one_card(session, card, page_num, sem, client) for card in cards]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    products: list[dict] = []
    ok = fail = 0
    for r in results:
        if isinstance(r, Exception) or r is None:
            fail += 1
        else:
            pid = (r.get("product_id") or "").strip()
            if pid:
                products.append(r)
                ok += 1
            else:
                fail += 1
    return products, ok, fail


# ════════════════════════════════════════════════════════════════════════════
# CRAWL ENGINE
# ════════════════════════════════════════════════════════════════════════════

class CrawlEngine:
    """
    Entity-centric crawl engine.

    DISCOVERY MODEL
      Each listing page is visited once per cycle.  For every card:
        • If product_id is in seen_ids → skip (O(1) set lookup)
        • Otherwise → enqueue for httpx detail fetch

      This means even if the same product appears on page 7 in cycle 1
      and on page 3 in cycle 2, it is only extracted once.

    STOP CONDITIONS (evaluated after each page, after each cycle)
      1. extracted_count >= expected_total * MIN_COVERAGE  (primary)
      2. consecutive_empty_cycles >= STOP_CYCLES           (safety)

    WHAT "EMPTY CYCLE" MEANS
      A full pass through all listing pages produced zero new product_ids.
      This is only meaningful after multiple cycles, not after a single page.
      The old "zero new products on THIS page → stop" was fatally wrong.

    RESUME
      On startup: load seen_ids from SQLite, read resume_page and
      current_cycle from crawl_state.  Jump browser to resume_page.
      On crash: SQLite is already up to date (committed per product).
    """

    def __init__(self, db: SrijanDB, writer: OutputWriter) -> None:
        self.db     = db
        self.writer = writer
        self.sem    = asyncio.Semaphore(CONCURRENCY)
        self.expected_total = 48200   # overridden from page UI on startup
        self.total_pages    = MAX_PAGES

    # ── Top-level run ──────────────────────────────────────────────────────────

    async def run(self) -> None:
        async with async_playwright() as pw:
            browser, page = await self._get_browser_page(pw)
            try:
                # ── Ensure we are on the marketplace ──────────────────────────
                on_site = await page.evaluate(
                    "() => location.href.includes('ProductMarketPlace')"
                )
                if not on_site:
                    print(f"Loading {TARGET} ...")
                    await page.goto(TARGET, wait_until="networkidle", timeout=60000)
                    await page.wait_for_selector("a[id*='LinkButton13']", timeout=20000)
                    print("Marketplace loaded.\n")

                # ── Read site totals ──────────────────────────────────────────
                total_prods, total_pages = await get_site_totals(page)
                if total_prods:
                    self.expected_total = total_prods
                if total_pages:
                    self.total_pages = total_pages
                print(f"  [INFO] Site reports: {self.expected_total:,} products, "
                      f"{self.total_pages} pages")
                self.db.set_state("expected_total", self.expected_total)
                self.db.set_state("total_pages", self.total_pages)

                # ── Resume state ──────────────────────────────────────────────
                cycle        = int(self.db.get_state("current_cycle", "1"))
                resume_page  = int(self.db.get_state("resume_page",   "1"))
                empty_cycles = int(self.db.get_state("empty_cycles",  "0"))
                extracted    = self.db.count()

                print(f"  [RESUME] Cycle {cycle} | resume page {resume_page} | "
                      f"extracted {extracted:,}/{self.expected_total:,} "
                      f"({100*extracted/self.expected_total:.1f}%)\n")

                # Jump to resume page if we are mid-cycle
                if resume_page > 1:
                    print(f"  Jumping to page {resume_page}...")
                    if not await jump_to_page(page, resume_page):
                        print(f"  [WARN] Jump to {resume_page} failed — restarting from page 1.")
                        resume_page = 1
                        self.db.set_state("resume_page", "1")

                # ── Main crawl loop ───────────────────────────────────────────
                async with httpx.AsyncClient(http2=False, follow_redirects=True) as client:
                    while True:
                        # Primary stop: coverage
                        extracted = self.db.count()
                        coverage  = extracted / self.expected_total
                        if coverage >= MIN_COVERAGE:
                            print(f"\n[STOP] Coverage {100*coverage:.2f}% ≥ "
                                  f"{100*MIN_COVERAGE:.1f}% — target reached.")
                            break

                        # Safety stop: too many empty cycles
                        if empty_cycles >= STOP_CYCLES:
                            print(f"\n[STOP] {empty_cycles} consecutive cycles with "
                                  f"zero new products — site fully covered.")
                            break

                        # Run one cycle
                        cycle_new = await self._run_cycle(
                            page, client, cycle, resume_page
                        )

                        resume_page = 1   # next cycle always starts from page 1
                        if cycle_new == 0:
                            empty_cycles += 1
                        else:
                            empty_cycles = 0

                        extracted = self.db.count()
                        print(
                            f"\n{'─'*60}\n"
                            f"  CYCLE {cycle} COMPLETE | New this cycle: {cycle_new:,} | "
                            f"Total: {extracted:,}/{self.expected_total:,} "
                            f"({100*extracted/self.expected_total:.1f}%) | "
                            f"Empty cycles: {empty_cycles}/{STOP_CYCLES}\n"
                            f"{'─'*60}\n"
                        )

                        cycle += 1
                        self.db.set_state("current_cycle", cycle)
                        self.db.set_state("resume_page",   "1")
                        self.db.set_state("empty_cycles",  empty_cycles)

            finally:
                self.writer.flush()
                try:
                    await browser.close()
                except Exception:
                    pass

    # ── Single cycle ───────────────────────────────────────────────────────────

    async def _run_cycle(
        self, page: Page, client: httpx.AsyncClient,
        cycle: int, start_page: int,
    ) -> int:
        """
        One pass through all listing pages.
        Returns total new products found across all pages.
        """
        cycle_new  = 0
        page_num   = start_page

        while page_num <= self.total_pages:
            new = await self._process_page(page, client, page_num, cycle)
            cycle_new += new

            # Coverage check after every page
            extracted = self.db.count()
            if extracted >= self.expected_total * MIN_COVERAGE:
                print(f"  [COVERAGE] {extracted:,}/{self.expected_total:,} — stopping cycle.")
                break

            # Advance browser
            await asyncio.sleep(PAGE_DELAY)
            has_next = await page.evaluate(
                "() => !!document.getElementById('ContentPlaceHolder1_lnkbtnPgNext')"
            )
            if not has_next:
                print(f"  [END] No next-page button at page {page_num}.")
                break

            advanced = await go_next_page(page)
            if not advanced:
                # Fallback 1: jump
                print(f"  [WARN] go_next_page failed at {page_num} — trying jump...")
                advanced = await jump_to_page(page, page_num + 1)
            if not advanced:
                # Fallback 2: full reload + jump
                print(f"  [RECOVERY] Reloading site and jumping to {page_num + 1}...")
                advanced = await self._reload_and_jump(page, page_num + 1)
            if not advanced:
                print(f"  [ERROR] Cannot advance past page {page_num} — ending cycle early.")
                break

            page_num += 1
            self.db.set_state("resume_page", str(page_num))

        return cycle_new

    # ── Single page ────────────────────────────────────────────────────────────

    async def _process_page(
        self, page: Page, client: httpx.AsyncClient,
        page_num: int, cycle: int,
    ) -> int:
        """
        Process one listing page:
          1. Extract all card identifiers (no clicks).
          2. Filter to unseen product_ids.
          3. Snapshot session for httpx.
          4. Concurrent httpx detail fetch.
          5. Dedup + store in SQLite.
          6. Buffer in OutputWriter.
        Returns count of newly extracted products.
        """
        t0 = time.monotonic()

        # 1. Read all cards from listing DOM (no postbacks)
        cards = await extract_listing_cards(page)
        if not cards:
            print(f"  Page {page_num:3d} (cycle {cycle}): no cards found — skipping.")
            self.db.log_page_visit(page_num, cycle, 0, 0, 0, 0)
            return 0

        # 2. Filter: keep only cards with unseen or unknown product_ids
        new_cards: list[dict] = []
        known = 0
        for card in cards:
            pid = (card.get("product_id") or "").strip()
            if pid and self.db.is_seen(pid):
                known += 1
            else:
                new_cards.append(card)

        print(f"  Page {page_num:3d} | cycle {cycle} | "
              f"{len(cards)} cards | {len(new_cards)} new | {known} skip")

        if not new_cards:
            self.db.log_page_visit(page_num, cycle, len(cards), 0, 0, 0)
            return 0

        # 3. Session snapshot
        session = await extract_page_session(page)
        print(f"          ViewState {len(session['viewstate'])} chars | "
              f"{len(session['extra_fields'])} fields | "
              f"fetching {len(new_cards)} via httpx...")

        # 4. Concurrent httpx fetch
        products, ok, fail = await fetch_cards_batch(
            session, new_cards, page_num, self.sem, client
        )

        # 5. Dedup + store (final check against DB in case of race between cards)
        new_count   = 0
        page_output: list[dict] = []
        for product in products:
            if self.db.insert_product(product):
                page_output.append(product)
                new_count += 1

        # 6. Write chunk
        if page_output:
            self.writer.add(page_output)

        elapsed = time.monotonic() - t0
        self.db.log_page_visit(page_num, cycle, len(cards), new_count, ok, fail)

        print(f"          → new:{new_count} | ok:{ok} | fail:{fail} | "
              f"{elapsed:.1f}s | total:{self.db.count():,}")
        return new_count

    # ── Browser lifecycle ──────────────────────────────────────────────────────

    async def _get_browser_page(self, pw) -> tuple[Browser, Page]:
        try:
            browser = await pw.chromium.connect_over_cdp(f"http://localhost:{CDP_PORT}")
            print(f"  [RESUME] Attached to Chrome on port {CDP_PORT}.")
            ctx  = browser.contexts[0] if browser.contexts else await browser.new_context()
            page = ctx.pages[0]        if ctx.pages        else await ctx.new_page()
            return browser, page
        except Exception:
            pass

        browser = await pw.chromium.launch(
            headless=False,
            args=[
                f"--remote-debugging-port={CDP_PORT}",
                "--disable-blink-features=AutomationControlled",
            ],
            handle_sigint=False,
            handle_sigterm=False,
            handle_sighup=False,
        )
        ctx  = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            )
        )
        page = await ctx.new_page()
        return browser, page

    async def _reload_and_jump(self, page: Page, target_page: int) -> bool:
        try:
            await page.goto(TARGET, wait_until="networkidle", timeout=60000)
            await page.wait_for_selector("a[id*='LinkButton13']", timeout=20000)
            return await jump_to_page(page, target_page)
        except Exception as exc:
            print(f"  [ERROR] Reload+jump failed: {exc}")
            return False


# ════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ════════════════════════════════════════════════════════════════════════════

async def main() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("=" * 66)
    print("  Srijan Entity-Centric Crawler")
    print(f"  Output dir : {OUTPUT_DIR}/")
    print(f"  State DB   : {DB_PATH}")
    print(f"  Concurrency: {CONCURRENCY} httpx threads per page")
    print(f"  Stop at    : {MIN_COVERAGE*100:.1f}% coverage OR "
          f"{STOP_CYCLES} empty cycles")
    print("=" * 66 + "\n")

    db     = SrijanDB(DB_PATH)
    writer = OutputWriter(OUTPUT_DIR)

    engine = CrawlEngine(db, writer)
    try:
        await engine.run()
    except KeyboardInterrupt:
        print(f"\n[INTERRUPTED] Chrome left open on port {CDP_PORT}.")
        print(f"  Extracted so far: {db.count():,}")
        print("  Run again to resume — state is saved in SQLite.")
    finally:
        writer.flush()
        db.close()

    print(f"\nFinal: {db.count():,} products extracted → {OUTPUT_DIR}/")


if __name__ == "__main__":
    asyncio.run(main())
