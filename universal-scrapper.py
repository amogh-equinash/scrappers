# universal-scrapper.py
# Scrapes products from 28 defence/aerospace OEM websites.
# Run: python3 universal-scrapper.py
#
# Two listing modes per site:
#   feed_url      → fetch a JSON product feed, iterate entries (fast)
#   base_url      → load the listing page and navigate from cards
#   listing_urls  → visit MULTIPLE listing pages (multi-category sites)
#
# detail_type controls how detail content loads after clicking a card:
#   "navigate"  → browser goes to a new URL
#   "ajax"      → content injects into the same page
#   "none"      → skip detail navigation (listing card content only)
#
# Output: oem-data/<site_key>.json  (directory auto-created)
# ─── CHANGE THIS to run one site or all ───────────────────────────────────────
ACTIVE_SITE = "all"   # "all" → every site in SITE_CONFIGS, else a single key
# ─────────────────────────────────────────────────────────────────────────────

import asyncio
import json
import os
import re
from pathlib import Path
from urllib.parse import urljoin, urlparse

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

OUTPUT_DIR = "oem-data"

# Injected into every page to mask headless Playwright fingerprint
STEALTH_JS = """
Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
Object.defineProperty(navigator, 'plugins',   {get: () => [1, 2, 3]});
Object.defineProperty(navigator, 'languages', {get: () => ['en-US','en']});
window.chrome = {runtime: {}};
"""

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# ── Site catalogue ────────────────────────────────────────────────────────────

SITE_CONFIGS: dict[str, dict] = {

    # ── 1. Lockheed Martin ────────────────────────────────────────────────────
    "lockheed_martin": {
        "base_url":   "https://www.lockheedmartin.com/en-us/products.html",
        "feed_url":   "https://www.lockheedmartin.com/data/feeds/productfeed.json",
        "feed_field_map": {
            "title":       "Title",
            "description": "Description",
            "date":        "Date",
            "url":         "URL",
            "thumbnail":   "Thumbnail Image",
            "tags":        "Tags",
            "domain":      "Domain",
            "country":     "Country",
        },
        "output_file":        f"{OUTPUT_DIR}/lockheed_martin.json",
        "max_products":       999999,
        "card_selector":      None,
        "card_link_selector": None,
        "detail_type":        "navigate",
        "detail_sentinel":    None,
        "next_page_selector": None,
        "listing_fields":     None,
        "detail_fields":      None,
        "wait_until":         "domcontentloaded",
        "settle_ms":          2500,
        "stealth":            False,
    },

    # ── 2. Roe.ru — handled by roe-scraper.py; stub here for reference ────────
    "roe_ru": {
        "base_url":       "https://roe.ru/production/land-forces/tanks/",
        "listing_urls": [
            "https://roe.ru/production/land-forces/tanks/",
            "https://roe.ru/production/land-forces/armored-combat-vehicles/",
            "https://roe.ru/production/land-forces/armored-vehicles/",
            "https://roe.ru/production/land-forces/small-arms-sv/pistols-sv/",
            "https://roe.ru/production/land-forces/small-arms-sv/assault-rifles-sv/",
            "https://roe.ru/production/land-forces/small-arms-sv/sniper-rifles-sv/",
            "https://roe.ru/production/aerospace-forces/aircraft/multipurpose-fighters-and-fighter-bombers/",
            "https://roe.ru/production/aerospace-forces/helicopters/combat-and-combat-transport-helicopters/",
            "https://roe.ru/production/navy/surface-ships-and-boats/frigates/",
            "https://roe.ru/production/navy/submarines/diesel-electric-submarines/",
            "https://roe.ru/production/protivovozdushnaya-oborona/sredstva-obnaruzheniya-vozdushnykh-tseley/radiolokatsionnye-stantsii-metrovogo-diapazona/",
        ],
        "feed_url":           None,
        "feed_field_map":     None,
        "output_file":        f"{OUTPUT_DIR}/roe_ru.json",
        "max_products":       999999,
        "card_selector":      "div.goods-card",
        "card_link_selector": "a.goods-card-image",
        "detail_type":        "navigate",
        "detail_sentinel":    None,
        "next_page_selector": None,
        "listing_fields":     None,
        "detail_fields":      None,
        "wait_until":         "domcontentloaded",
        "settle_ms":          1500,
        "stealth":            True,
    },

    # ── 3. RTX / Raytheon ─────────────────────────────────────────────────────
    "rtx": {
        "base_url":    "https://www.rtx.com/raytheon/what-we-do",
        "listing_urls": [
            "https://www.rtx.com/raytheon/what-we-do/air",
            "https://www.rtx.com/raytheon/what-we-do/land",
            "https://www.rtx.com/raytheon/what-we-do/sea",
            "https://www.rtx.com/raytheon/what-we-do/cyber-space",
        ],
        "feed_url":           None,
        "feed_field_map":     None,
        "output_file":        f"{OUTPUT_DIR}/rtx.json",
        "max_products":       999999,
        "card_selector":      None,
        "card_link_selector": None,
        "detail_type":        "navigate",
        "detail_sentinel":    None,
        "next_page_selector": None,
        "listing_fields":     None,
        "detail_fields":      None,
        "wait_until":         "load",
        "settle_ms":          3000,
        "stealth":            True,
    },

    # ── 4. MBDA ───────────────────────────────────────────────────────────────
    "mbda": {
        "base_url":    "https://www.mbda-systems.com/products/",
        "listing_urls": [
            "https://www.mbda-systems.com/products/",
            "https://www.mbda-systems.com/products/deep-strike/",
            "https://www.mbda-systems.com/products/air-dominance/",
            "https://www.mbda-systems.com/products/surface-combat/",
            "https://www.mbda-systems.com/products/maritime/",
            "https://www.mbda-systems.com/products/counter-uav/",
            "https://www.mbda-systems.com/products/training-and-simulation/",
        ],
        "feed_url":           None,
        "feed_field_map":     None,
        "output_file":        f"{OUTPUT_DIR}/mbda.json",
        "max_products":       999999,
        "card_selector":      ".product-item",
        "card_link_selector": "a",
        "detail_type":        "navigate",
        "detail_sentinel":    None,
        "next_page_selector": None,
        "listing_fields":     None,
        "detail_fields":      None,
        "wait_until":         "load",
        "settle_ms":          2500,
        "stealth":            True,
    },

    # ── 5. Rafael Advanced Defense Systems ───────────────────────────────────
    "rafael": {
        "base_url":    "https://www.rafael.co.il/systems/",
        "listing_urls": [
            "https://www.rafael.co.il/systems/",
            "https://www.rafael.co.il/arena-of-systems/air-and-missile-defence/",
            "https://www.rafael.co.il/arena-of-systems/land/",
            "https://www.rafael.co.il/arena-of-systems/naval/",
            "https://www.rafael.co.il/arena-of-systems/air/",
            "https://www.rafael.co.il/arena-of-systems/cyber-and-intelligence/",
            "https://www.rafael.co.il/arena-of-systems/uas-counter-uas/",
        ],
        "feed_url":           None,
        "feed_field_map":     None,
        "output_file":        f"{OUTPUT_DIR}/rafael.json",
        "max_products":       999999,
        "card_selector":      None,
        "card_link_selector": None,
        "detail_type":        "navigate",
        "detail_sentinel":    None,
        "next_page_selector": None,
        "listing_fields":     None,
        "detail_fields":      None,
        "wait_until":         "load",
        "settle_ms":          4000,
        "stealth":            True,
    },

    # ── 6. Kongsberg Defence & Aerospace ──────────────────────────────────────
    "kongsberg": {
        "base_url":    "https://www.kongsberg.com/kda/products-and-services/",
        "listing_urls": [
            "https://www.kongsberg.com/kda/products-and-services/",
            "https://www.kongsberg.com/kda/products-and-services/defence-and-security/",
            "https://www.kongsberg.com/kda/products-and-services/missile-systems/",
            "https://www.kongsberg.com/kda/products-and-services/air-defence/",
            "https://www.kongsberg.com/kda/products-and-services/remote-weapon-stations/",
        ],
        "feed_url":           None,
        "feed_field_map":     None,
        "output_file":        f"{OUTPUT_DIR}/kongsberg.json",
        "max_products":       999999,
        "card_selector":      None,
        "card_link_selector": None,
        "detail_type":        "navigate",
        "detail_sentinel":    None,
        "next_page_selector": None,
        "listing_fields":     None,
        "detail_fields":      None,
        "wait_until":         "load",
        "settle_ms":          3500,
        "stealth":            True,
    },

    # ── 7. Bharat Dynamics Limited (BDL) ─────────────────────────────────────
    "bdl_india": {
        "base_url":    "https://bdl-india.in/products/",
        "listing_urls": [
            "https://bdl-india.in/products/",
            "https://bdl-india.in/anti-tank-guided-missiles/",
            "https://bdl-india.in/air-to-air-missiles/",
            "https://bdl-india.in/surface-to-air-missiles/",
            "https://bdl-india.in/underwater-weapons/",
            "https://bdl-india.in/launchers/",
        ],
        "feed_url":           None,
        "feed_field_map":     None,
        "output_file":        f"{OUTPUT_DIR}/bdl_india.json",
        "max_products":       999999,
        "card_selector":      None,
        "card_link_selector": None,
        "detail_type":        "navigate",
        "detail_sentinel":    None,
        "next_page_selector": None,
        "listing_fields":     None,
        "detail_fields":      None,
        "wait_until":         "load",
        "settle_ms":          3000,
        "stealth":            True,
    },

    # ── 8. BrahMos Aerospace ──────────────────────────────────────────────────
    "brahmos": {
        "base_url":    "https://www.brahmos.com/products.php",
        "listing_urls": [
            "https://www.brahmos.com/products.php",
            "https://www.brahmos.com/content.php?id=10",
            "https://www.brahmos.com/content.php?id=11",
            "https://www.brahmos.com/content.php?id=12",
        ],
        "feed_url":           None,
        "feed_field_map":     None,
        "output_file":        f"{OUTPUT_DIR}/brahmos.json",
        "max_products":       999999,
        "card_selector":      None,
        "card_link_selector": None,
        "detail_type":        "navigate",
        "detail_sentinel":    None,
        "next_page_selector": None,
        "listing_fields":     None,
        "detail_fields":      None,
        "wait_until":         "load",
        "settle_ms":          3500,
        "stealth":            True,
    },

    # ── 9. Rostec ─────────────────────────────────────────────────────────────
    "rostec": {
        "base_url":    "https://rostec.ru/en/directions/weapons/",
        "listing_urls": [
            "https://rostec.ru/en/directions/weapons/",
            "https://rostec.ru/en/directions/weapons/projects",
            "https://rostec.ru/en/directions/aviation/",
            "https://rostec.ru/en/directions/radioelectronics/",
            "https://rostec.ru/en/directions/armored/",
        ],
        "feed_url":           None,
        "feed_field_map":     None,
        "output_file":        f"{OUTPUT_DIR}/rostec.json",
        "max_products":       999999,
        "card_selector":      "[class*='product']",
        "card_link_selector": "a",
        "detail_type":        "navigate",
        "detail_sentinel":    None,
        "next_page_selector": None,
        "listing_fields":     None,
        "detail_fields":      None,
        "wait_until":         "load",
        "settle_ms":          2500,
        "stealth":            True,
    },

    # ── 10. NORINCO ───────────────────────────────────────────────────────────
    "norinco": {
        "base_url":    "http://eng.norinco.cn/products.html",
        "listing_urls": [
            "http://eng.norinco.cn/products.html",
            "http://eng.norinco.cn/SL/",
            "http://eng.norinco.cn/ZJ/",
            "http://eng.norinco.cn/PZ/",
        ],
        "feed_url":           None,
        "feed_field_map":     None,
        "output_file":        f"{OUTPUT_DIR}/norinco.json",
        "max_products":       999999,
        "card_selector":      None,
        "card_link_selector": None,
        "detail_type":        "navigate",
        "detail_sentinel":    None,
        "next_page_selector": None,
        "listing_fields":     None,
        "detail_fields":      None,
        "wait_until":         "load",
        "settle_ms":          4000,
        "stealth":            True,
    },

    # ── 11. Aselsan ───────────────────────────────────────────────────────────
    "aselsan": {
        "base_url":    "https://www.aselsan.com/en/systems",
        "listing_urls": [
            "https://www.aselsan.com/en/systems",
            "https://www.aselsan.com/en/products",
            "https://www.aselsan.com/en/systems/land-systems",
            "https://www.aselsan.com/en/systems/naval-systems",
            "https://www.aselsan.com/en/systems/air-defense",
            "https://www.aselsan.com/en/systems/electronic-warfare",
            "https://www.aselsan.com/en/systems/communication-information",
        ],
        "feed_url":           None,
        "feed_field_map":     None,
        "output_file":        f"{OUTPUT_DIR}/aselsan.json",
        "max_products":       999999,
        "card_selector":      None,
        "card_link_selector": None,
        "detail_type":        "navigate",
        "detail_sentinel":    None,
        "next_page_selector": None,
        "listing_fields":     None,
        "detail_fields":      None,
        "wait_until":         "load",
        "settle_ms":          5000,
        "stealth":            True,
    },

    # ── 12. Boeing Defense ────────────────────────────────────────────────────
    "boeing_defense": {
        "base_url":    "https://www.boeing.com/defense/",
        "listing_urls": [
            "https://www.boeing.com/defense/autonomous-and-unmanned-systems",
            "https://www.boeing.com/defense/weapon-systems",
            "https://www.boeing.com/defense/fighters-and-bombers",
            "https://www.boeing.com/defense/military-rotorcraft",
            "https://www.boeing.com/defense/tankers-and-transports",
            "https://www.boeing.com/defense/patrol-early-warning-and-battle-management",
            "https://www.boeing.com/defense/missile-systems",
            "https://www.boeing.com/defense/ground-systems",
        ],
        "feed_url":           None,
        "feed_field_map":     None,
        "output_file":        f"{OUTPUT_DIR}/boeing_defense.json",
        "max_products":       999999,
        "card_selector":      "[class*='card']",
        "card_link_selector": "a",
        "detail_type":        "navigate",
        "detail_sentinel":    None,
        "next_page_selector": None,
        "listing_fields":     None,
        "detail_fields":      None,
        "wait_until":         "load",
        "settle_ms":          3000,
        "stealth":            True,
    },

    # ── 13. Northrop Grumman ──────────────────────────────────────────────────
    "northrop_grumman": {
        "base_url":    "https://www.northropgrumman.com/what-we-do/",
        "listing_urls": [
            "https://www.northropgrumman.com/what-we-do/advanced-weapons/",
            "https://www.northropgrumman.com/what-we-do/missile-defense/",
            "https://www.northropgrumman.com/what-we-do/mission-solutions/electronic-warfare/",
            "https://www.northropgrumman.com/what-we-do/air/",
            "https://www.northropgrumman.com/what-we-do/space/",
            "https://www.northropgrumman.com/what-we-do/land/",
        ],
        "feed_url":           None,
        "feed_field_map":     None,
        "output_file":        f"{OUTPUT_DIR}/northrop_grumman.json",
        "max_products":       999999,
        "card_selector":      None,
        "card_link_selector": None,
        "detail_type":        "navigate",
        "detail_sentinel":    None,
        "next_page_selector": None,
        "listing_fields":     None,
        "detail_fields":      None,
        "wait_until":         "load",
        "settle_ms":          3000,
        "stealth":            True,
    },

    # ── 14. Dassault Aviation ─────────────────────────────────────────────────
    "dassault": {
        "base_url":    "https://www.dassault-aviation.com/en/defense/",
        "listing_urls": [
            "https://www.dassault-aviation.com/en/defense/",
            "https://www.dassault-aviation.com/en/defense/rafale/",
            "https://www.dassault-aviation.com/en/defense/falcon-military/",
            "https://www.dassault-aviation.com/en/defense/neuron/",
        ],
        "feed_url":           None,
        "feed_field_map":     None,
        "output_file":        f"{OUTPUT_DIR}/dassault.json",
        "max_products":       999999,
        "card_selector":      "[class*='card__']",
        "card_link_selector": "a",
        "detail_type":        "navigate",
        "detail_sentinel":    None,
        "next_page_selector": None,
        "listing_fields":     None,
        "detail_fields":      None,
        "wait_until":         "load",
        "settle_ms":          3000,
        "stealth":            True,
    },

    # ── 15. Saab ──────────────────────────────────────────────────────────────
    "saab": {
        "base_url":    "https://www.saab.com/products/",
        "listing_urls": [
            "https://www.saab.com/products/air/",
            "https://www.saab.com/products/land/",
            "https://www.saab.com/products/naval/",
            "https://www.saab.com/products/security/",
            "https://www.saab.com/products/product-search/",
        ],
        "feed_url":           None,
        "feed_field_map":     None,
        "output_file":        f"{OUTPUT_DIR}/saab.json",
        "max_products":       999999,
        "card_selector":      None,
        "card_link_selector": None,
        "detail_type":        "navigate",
        "detail_sentinel":    None,
        "next_page_selector": "a[rel='next'], .pagination__next",
        "listing_fields":     None,
        "detail_fields":      None,
        "wait_until":         "load",
        "settle_ms":          3000,
        "stealth":            True,
    },

    # ── 16. Airbus Defence & Space ────────────────────────────────────────────
    "airbus_defence": {
        "base_url":    "https://www.airbus.com/en/products-services/defence",
        "listing_urls": [
            "https://www.airbus.com/en/products-services/defence",
            "https://www.airbus.com/en/products-services/defence/air-power",
            "https://www.airbus.com/en/products-services/defence/space",
            "https://www.airbus.com/en/products-services/defence/connected-intelligence",
            "https://www.airbus.com/en/products-services/defence/c2-air-traffic-management",
        ],
        "feed_url":           None,
        "feed_field_map":     None,
        "output_file":        f"{OUTPUT_DIR}/airbus_defence.json",
        "max_products":       999999,
        "card_selector":      None,
        "card_link_selector": None,
        "detail_type":        "navigate",
        "detail_sentinel":    None,
        "next_page_selector": None,
        "listing_fields":     None,
        "detail_fields":      None,
        "wait_until":         "load",
        "settle_ms":          3500,
        "stealth":            True,
    },

    # ── 17. HAL India ─────────────────────────────────────────────────────────
    "hal_india": {
        "base_url":    "https://hal-india.co.in/Major-Products-Programs/56",
        "listing_urls": [
            "https://hal-india.co.in/Major-Products-Programs/56",
            "https://hal-india.co.in/Major-Products-Programs/57",
            "https://hal-india.co.in/Major-Products-Programs/58",
            "https://hal-india.co.in/Major-Products-Programs/59",
            "https://hal-india.co.in/Major-Products-Programs/60",
            "https://hal-india.co.in/Major-Products-Programs/61",
        ],
        "feed_url":           None,
        "feed_field_map":     None,
        "output_file":        f"{OUTPUT_DIR}/hal_india.json",
        "max_products":       999999,
        "card_selector":      None,
        "card_link_selector": None,
        "detail_type":        "navigate",
        "detail_sentinel":    None,
        "next_page_selector": None,
        "listing_fields":     None,
        "detail_fields":      None,
        "wait_until":         "load",
        "settle_ms":          4000,
        "stealth":            True,
    },

    # ── 18. Leonardo Defence ──────────────────────────────────────────────────
    "leonardo": {
        "base_url":    "https://www.leonardo.com/en/defence",
        "listing_urls": [
            "https://www.leonardo.com/en/defence",
            "https://www.leonardo.com/en/defence/land-and-naval-defence",
            "https://www.leonardo.com/en/defence/electronics-defence",
            "https://www.leonardo.com/en/aeronautics",
            "https://www.leonardo.com/en/space",
            "https://www.leonardo.com/en/helicopters",
        ],
        "feed_url":           None,
        "feed_field_map":     None,
        "output_file":        f"{OUTPUT_DIR}/leonardo.json",
        "max_products":       999999,
        "card_selector":      "[class*='product-card'], [class*='card']",
        "card_link_selector": "a",
        "detail_type":        "navigate",
        "detail_sentinel":    None,
        "next_page_selector": None,
        "listing_fields":     None,
        "detail_fields":      None,
        "wait_until":         "load",
        "settle_ms":          4000,
        "stealth":            True,
    },

    # ── 19. General Dynamics Land Systems (GDLS) ──────────────────────────────
    "gdls": {
        "base_url":    "https://www.gdls.com",
        "listing_urls": [
            "https://www.gdls.com",
            "https://www.gdls.com/our-vehicles/",
            "https://www.gdls.com/capabilities/",
        ],
        "feed_url":           None,
        "feed_field_map":     None,
        "output_file":        f"{OUTPUT_DIR}/gdls.json",
        "max_products":       999999,
        "card_selector":      None,
        "card_link_selector": None,
        "detail_type":        "navigate",
        "detail_sentinel":    None,
        "next_page_selector": None,
        "listing_fields":     None,
        "detail_fields":      None,
        "wait_until":         "load",
        "settle_ms":          4000,
        "stealth":            True,
    },

    # ── 20. BAE Systems ───────────────────────────────────────────────────────
    "bae_systems": {
        "base_url":    "https://www.baesystems.com/en/products",
        "listing_urls": [
            "https://www.baesystems.com/en/products",
            "https://www.baesystems.com/en/capabilities/electronic-systems",
            "https://www.baesystems.com/en/capabilities/combat-vehicles",
            "https://www.baesystems.com/en/capabilities/platforms-and-services",
            "https://www.baesystems.com/en/capabilities/air",
            "https://www.baesystems.com/en/capabilities/maritime",
        ],
        "feed_url":           None,
        "feed_field_map":     None,
        "output_file":        f"{OUTPUT_DIR}/bae_systems.json",
        "max_products":       999999,
        "card_selector":      None,
        "card_link_selector": None,
        "detail_type":        "navigate",
        "detail_sentinel":    None,
        "next_page_selector": None,
        "listing_fields":     None,
        "detail_fields":      None,
        "wait_until":         "load",
        "settle_ms":          4000,
        "stealth":            True,
    },

    # ── 21. Rheinmetall ───────────────────────────────────────────────────────
    "rheinmetall": {
        "base_url":    "https://www.rheinmetall.com/en/products/land/overview",
        "listing_urls": [
            "https://www.rheinmetall.com/en/products/land/overview",
            "https://www.rheinmetall.com/en/products/air/overview",
            "https://www.rheinmetall.com/en/products/sea/overview",
            "https://www.rheinmetall.com/en/products/digitization/overview",
            "https://www.rheinmetall.com/en/products/training-and-service/overview",
            "https://www.rheinmetall.com/en/products/uncrewed-systems-and-autonomous-navigation-technology",
        ],
        "feed_url":           None,
        "feed_field_map":     None,
        "output_file":        f"{OUTPUT_DIR}/rheinmetall.json",
        "max_products":       999999,
        "card_selector":      None,
        "card_link_selector": None,
        "detail_type":        "navigate",
        "detail_sentinel":    None,
        "next_page_selector": None,
        "listing_fields":     None,
        "detail_fields":      None,
        "wait_until":         "load",
        "settle_ms":          3500,
        "stealth":            True,
    },

    # ── 22. Elbit Systems ─────────────────────────────────────────────────────
    "elbit": {
        "base_url":    "https://elbitsystems.com/our-solutions/",
        "listing_urls": [
            "https://elbitsystems.com/our-solutions/",
            "https://elbitsystems.com/our-solutions/land/",
            "https://elbitsystems.com/our-solutions/air/",
            "https://elbitsystems.com/our-solutions/naval/",
            "https://elbitsystems.com/our-solutions/airborne-systems/",
            "https://elbitsystems.com/our-solutions/c4i-and-cyber/",
            "https://elbitsystems.com/our-solutions/uas/",
        ],
        "feed_url":           None,
        "feed_field_map":     None,
        "output_file":        f"{OUTPUT_DIR}/elbit.json",
        "max_products":       999999,
        "card_selector":      None,
        "card_link_selector": None,
        "detail_type":        "navigate",
        "detail_sentinel":    None,
        "next_page_selector": None,
        "listing_fields":     None,
        "detail_fields":      None,
        "wait_until":         "load",
        "settle_ms":          4000,
        "stealth":            True,
    },

    # ── 23. Hanwha Aerospace ──────────────────────────────────────────────────
    "hanwha": {
        "base_url":    "https://www.hanwhaaerospace.com/en/index.do",
        "listing_urls": [
            "https://www.hanwhaaerospace.com/en/index.do",
            "https://www.hanwhaaerospace.com/en/product/defense.do",
            "https://www.hanwhaaerospace.com/en/product/aviation.do",
        ],
        "feed_url":           None,
        "feed_field_map":     None,
        "output_file":        f"{OUTPUT_DIR}/hanwha.json",
        "max_products":       999999,
        "card_selector":      None,
        "card_link_selector": None,
        "detail_type":        "navigate",
        "detail_sentinel":    None,
        "next_page_selector": None,
        "listing_fields":     None,
        "detail_fields":      None,
        "wait_until":         "load",
        "settle_ms":          5000,
        "stealth":            True,
    },

    # ── 24. Anduril Industries ────────────────────────────────────────────────
    "anduril": {
        "base_url":    "https://www.anduril.com",
        "listing_urls": [
            "https://www.anduril.com",
            "https://www.anduril.com/capabilities/",
            "https://www.anduril.com/article/lattice/",
        ],
        "feed_url":           None,
        "feed_field_map":     None,
        "output_file":        f"{OUTPUT_DIR}/anduril.json",
        "max_products":       999999,
        "card_selector":      None,
        "card_link_selector": None,
        "detail_type":        "navigate",
        "detail_sentinel":    None,
        "next_page_selector": None,
        "listing_fields":     None,
        "detail_fields":      None,
        "wait_until":         "load",
        "settle_ms":          5000,
        "stealth":            True,
    },

    # ── 25. Shield AI ─────────────────────────────────────────────────────────
    "shield_ai": {
        "base_url":    "https://shield.ai",
        "listing_urls": [
            "https://shield.ai",
            "https://shield.ai/hivemind-solutions/",
            "https://shield.ai/technology/",
        ],
        "feed_url":           None,
        "feed_field_map":     None,
        "output_file":        f"{OUTPUT_DIR}/shield_ai.json",
        "max_products":       999999,
        "card_selector":      None,
        "card_link_selector": None,
        "detail_type":        "navigate",
        "detail_sentinel":    None,
        "next_page_selector": None,
        "listing_fields":     None,
        "detail_fields":      None,
        "wait_until":         "load",
        "settle_ms":          5000,
        "stealth":            True,
    },

    # ── 26. General Atomics Aeronautical (GA-ASI) ─────────────────────────────
    "ga_asi": {
        "base_url":    "https://www.ga-asi.com/products-services",
        "listing_urls": [
            "https://www.ga-asi.com/products-services",
            "https://www.ga-asi.com/remotely-piloted-aircraft",
            "https://www.ga-asi.com/sensor-systems",
            "https://www.ga-asi.com/weapons",
            "https://www.ga-asi.com/ground-systems",
        ],
        "feed_url":           None,
        "feed_field_map":     None,
        "output_file":        f"{OUTPUT_DIR}/ga_asi.json",
        "max_products":       999999,
        "card_selector":      "[class*='card']",
        "card_link_selector": "a",
        "detail_type":        "navigate",
        "detail_sentinel":    None,
        "next_page_selector": None,
        "listing_fields":     None,
        "detail_fields":      None,
        "wait_until":         "domcontentloaded",
        "settle_ms":          2500,
        "stealth":            True,
    },

    # ── 27. Baykar Technologies ───────────────────────────────────────────────
    "baykar": {
        "base_url":    "https://baykartech.com/en/",
        "listing_urls": [
            "https://baykartech.com/en/unmanned-aerial-vehicle-systems/",
            "https://baykartech.com/en/avionics-subsystems/",
            "https://baykartech.com/en/payload-systems/",
            "https://baykartech.com/en/simulator-system/",
        ],
        "feed_url":           None,
        "feed_field_map":     None,
        "output_file":        f"{OUTPUT_DIR}/baykar.json",
        "max_products":       999999,
        "card_selector":      None,
        "card_link_selector": None,
        "detail_type":        "navigate",
        "detail_sentinel":    None,
        "next_page_selector": None,
        "listing_fields":     None,
        "detail_fields":      None,
        "wait_until":         "load",
        "settle_ms":          3000,
        "stealth":            True,
    },

    # ── 28. AeroVironment ─────────────────────────────────────────────────────
    "aerovironment": {
        "base_url":    "https://www.avinc.com",
        "listing_urls": [
            "https://www.avinc.com/solutions",
            "https://www.avinc.com/domains/land/",
            "https://www.avinc.com/domains/air/",
            "https://www.avinc.com/domains/sea/",
        ],
        "feed_url":           None,
        "feed_field_map":     None,
        "output_file":        f"{OUTPUT_DIR}/aerovironment.json",
        "max_products":       999999,
        "card_selector":      "[class*='card__']",
        "card_link_selector": "a",
        "detail_type":        "navigate",
        "detail_sentinel":    None,
        "next_page_selector": None,
        "listing_fields":     None,
        "detail_fields":      None,
        "wait_until":         "domcontentloaded",
        "settle_ms":          2500,
        "stealth":            True,
    },

    # ── 29. BEL India ─────────────────────────────────────────────────────────
    "bel_india": {
        "base_url":    "https://bel-india.in",
        "listing_urls": [
            "https://bel-india.in",
            "https://bel-india.in/products/",
            "https://bel-india.in/radar-and-fire-control-systems/",
            "https://bel-india.in/naval-systems/",
            "https://bel-india.in/electronic-warfare/",
            "https://bel-india.in/communication-systems/",
        ],
        "feed_url":           None,
        "feed_field_map":     None,
        "output_file":        f"{OUTPUT_DIR}/bel_india.json",
        "max_products":       999999,
        "card_selector":      None,
        "card_link_selector": None,
        "detail_type":        "navigate",
        "detail_sentinel":    None,
        "next_page_selector": None,
        "listing_fields":     None,
        "detail_fields":      None,
        "wait_until":         "load",
        "settle_ms":          3500,
        "stealth":            True,
    },
}

# ── Auto-probe card selectors (when card_selector is None) ────────────────────
CARD_PROBE_SELECTORS = [
    "article.product", ".product-card", ".product-item", "li.product",
    "[class*='product-card']", "[class*='ProductCard']", "[class*='product_card']",
    "[class*='product-item']", "[class*='solution-card']", "[class*='system-card']",
    "[class*='card__']", "[class*='card-body']", "[class*='card-item']",
    "article", ".card", "li.item", "[data-product]", "[data-item]",
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def abs_url(base: str, href: str | None) -> str | None:
    if not href:
        return None
    if href.startswith(("data:", "javascript:", "#", "mailto:")):
        return None
    return urljoin(base, href)


def clean(text: str | None) -> str | None:
    if not text:
        return None
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def resolve_field(entry: dict, key: str):
    parts = key.split(".", 1)
    val = entry.get(parts[0])
    if len(parts) == 2 and isinstance(val, dict):
        return val.get(parts[1])
    return val


def same_origin(url: str, base: str) -> bool:
    return urlparse(url).netloc == urlparse(base).netloc


# ── Feed-based listing ────────────────────────────────────────────────────────

async def fetch_feed_stubs(page, cfg: dict) -> list[dict]:
    response = await page.request.get(cfg["feed_url"])
    if not response.ok:
        raise RuntimeError(f"Feed HTTP {response.status}: {cfg['feed_url']}")

    entries = await response.json()
    if not isinstance(entries, list):
        for key in ("items", "products", "results", "data"):
            if key in entries and isinstance(entries[key], list):
                entries = entries[key]
                break

    stubs = []
    field_map: dict = cfg.get("feed_field_map") or {}
    base = cfg["base_url"]

    for entry in entries:
        stub: dict = {}
        if field_map:
            for out_key, src_key in field_map.items():
                val = resolve_field(entry, src_key)
                if out_key in ("url", "thumbnail") and isinstance(val, str):
                    val = abs_url(base, val)
                stub[out_key] = val
        else:
            stub = dict(entry)
        stubs.append(stub)

    return stubs


# ── Card-based listing ────────────────────────────────────────────────────────

async def best_card_selector(page) -> str | None:
    best_sel, best_count = None, 1
    for sel in CARD_PROBE_SELECTORS:
        try:
            count = await page.locator(sel).count()
            if count > best_count:
                best_sel, best_count = sel, count
        except Exception:
            pass
    return best_sel


async def extract_card_stub(card_el, cfg: dict, base_url: str) -> dict:
    stub: dict = {}

    if cfg.get("listing_fields"):
        for field, sel in cfg["listing_fields"].items():
            el = await card_el.query_selector(sel)
            stub[field] = clean(await el.inner_text()) if el else None
    else:
        for sel in ("h1", "h2", "h3", "h4"):
            el = await card_el.query_selector(sel)
            if el:
                stub["title"] = clean(await el.inner_text())
                break
        for sel in ("p", "[class*='summary']", "[class*='desc']", "span"):
            el = await card_el.query_selector(sel)
            if el:
                text = clean(await el.inner_text())
                if text and text != stub.get("title"):
                    stub["summary"] = text
                    break
        img = await card_el.query_selector("img")
        if img:
            src = await img.get_attribute("src") or await img.get_attribute("data-src")
            stub["image_url"] = abs_url(base_url, src)

    link_sel = cfg.get("card_link_selector")
    link_el = await card_el.query_selector(link_sel if link_sel else "a")
    if link_el:
        href = await link_el.get_attribute("href")
        stub["url"] = abs_url(base_url, href)

    return stub


# ── Detail extraction ─────────────────────────────────────────────────────────

async def extract_detail_generic(page, cfg: dict, base_url: str) -> dict:
    data: dict = {}

    if cfg.get("detail_fields"):
        for field, sel in cfg["detail_fields"].items():
            el = await page.query_selector(sel)
            data[field] = clean(await el.inner_text()) if el else None
        return data

    # 1. <dl> definition lists
    for dl in await page.query_selector_all("dl"):
        dts = await dl.query_selector_all("dt")
        dds = await dl.query_selector_all("dd")
        for dt, dd in zip(dts, dds):
            key = clean(await dt.inner_text())
            val = clean(await dd.inner_text())
            if key and val:
                data.setdefault(key, val)

    # 2. Two-column tables
    for row in await page.query_selector_all("table tr"):
        cells = await row.query_selector_all("th, td")
        if len(cells) == 2:
            key = clean((await cells[0].inner_text()).rstrip(":"))
            val = clean(await cells[1].inner_text())
            if key and val:
                data.setdefault(key, val)

    # 3. Heading → next-sibling paragraph
    for h_sel in ("h2", "h3"):
        for h in await page.query_selector_all(h_sel):
            key = clean(await h.inner_text())
            if not key or len(key) > 120:
                continue
            sib = await page.evaluate(
                "(el) => el.nextElementSibling ? el.nextElementSibling.innerText : ''", h
            )
            val = clean(sib)
            if val:
                data.setdefault(key, val)

    h1 = await page.query_selector("h1")
    if h1:
        data.setdefault("detail_title", clean(await h1.inner_text()))

    main = await page.query_selector(
        "main, [role='main'], #main-content, .content-well, .page-content, article"
    )
    if main:
        full_text = clean(await main.inner_text())
        if full_text:
            data.setdefault("full_text", full_text[:5000])

    SKIP_PAT = ("logo", "icon", "sprite", "pixel", "blank", "close", "template/images",
                "arrow", "chevron", "hamburger", "favicon")
    images = []
    for img in await page.query_selector_all("img"):
        src = await img.get_attribute("src") or await img.get_attribute("data-src") or ""
        alt = await img.get_attribute("alt") or ""
        url = abs_url(base_url, src)
        if url and not any(p in url.lower() for p in SKIP_PAT):
            images.append({"url": url, "alt": clean(alt)})
    if images:
        data["images"] = images[:20]

    if "full_text" not in data:
        meta = await page.query_selector("meta[name='description']")
        if meta:
            content = await meta.get_attribute("content")
            if content:
                data["description"] = clean(content)

    return data


# ── Navigation helpers ────────────────────────────────────────────────────────

async def load_page(page, url: str, cfg: dict):
    await page.goto(url, wait_until=cfg.get("wait_until", "domcontentloaded"), timeout=90_000)
    await asyncio.sleep(cfg.get("settle_ms", 1500) / 1000)


async def wait_for_detail(page, cfg: dict, pre_url: str):
    settle = cfg.get("settle_ms", 1500)
    sentinel = cfg.get("detail_sentinel")
    dtype = cfg.get("detail_type", "navigate")

    if dtype == "navigate":
        try:
            await page.wait_for_url(lambda u: u != pre_url, timeout=20_000)
        except PlaywrightTimeout:
            pass
        await asyncio.sleep(settle / 1000)

    elif dtype == "ajax":
        if sentinel:
            try:
                await page.wait_for_function(
                    f"document.querySelector('{sentinel}') && "
                    f"document.querySelector('{sentinel}').innerText.trim().length > 0",
                    timeout=30_000,
                )
            except PlaywrightTimeout:
                pass
        else:
            try:
                await page.wait_for_load_state("networkidle", timeout=20_000)
            except PlaywrightTimeout:
                pass
        await asyncio.sleep(settle / 1000)


def _save(items: list, path: str):
    os.makedirs(os.path.dirname(path) if os.path.dirname(path) else ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(items, f, indent=2, ensure_ascii=False)
    print(f"  → Written {path}  ({len(items)} products)")


# ── Per-site scrape ───────────────────────────────────────────────────────────

async def scrape_site(site_key: str, cfg: dict, browser) -> list[dict]:
    base_url    = cfg["base_url"]
    output_file = cfg["output_file"]
    collected: list[dict] = []

    ctx = await browser.new_context(
        user_agent=USER_AGENT,
        viewport={"width": 1440, "height": 900},
        locale="en-US",
        extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
    )
    await ctx.add_init_script(STEALTH_JS)

    page = await ctx.new_page()

    print(f"\n{'='*60}")
    print(f"  Site: {site_key.upper()}")
    print(f"  URL:  {base_url}")
    print(f"{'='*60}")

    try:
        # ── Mode A: Feed-based listing ─────────────────────────────────────
        if cfg.get("feed_url"):
            print(f"  Fetching JSON feed …")
            await page.goto(base_url, wait_until=cfg.get("wait_until", "domcontentloaded"),
                            timeout=60_000)
            stubs = await fetch_feed_stubs(page, cfg)
            print(f"  Feed returned {len(stubs)} entries")

            for i, stub in enumerate(stubs):
                detail_url = stub.get("url")
                title = stub.get("title") or detail_url or f"product-{i+1}"
                print(f"  [{i+1}/{len(stubs)}] {title}")

                if not detail_url:
                    stub["detail_error"] = "no URL in feed entry"
                    collected.append(stub)
                    continue

                detail: dict = {}
                for attempt in range(1, 3):
                    try:
                        await load_page(page, detail_url, cfg)
                        detail = await extract_detail_generic(page, cfg, base_url)
                        detail["detail_url"] = page.url
                        break
                    except Exception as exc:
                        if attempt == 2:
                            detail["detail_error"] = str(exc)
                        else:
                            await asyncio.sleep(2)

                stub.update(detail)
                collected.append(stub)

        # ── Mode B: Card-based listing (single or multiple listing URLs) ───
        else:
            listing_urls = cfg.get("listing_urls") or [base_url]
            seen_detail_urls: set[str] = set()

            card_sel = cfg.get("card_selector")
            cfg_resolved = dict(cfg)

            for listing_url in listing_urls:

                print(f"\n  Listing: {listing_url}")
                try:
                    await load_page(page, listing_url, cfg)
                except Exception as exc:
                    print(f"    [warn] failed to load listing: {exc}")
                    continue

                # Auto-detect card selector on first listing page if not set
                if not card_sel:
                    print("    Auto-detecting card selector …")
                    card_sel = await best_card_selector(page)
                    if card_sel:
                        print(f"    → Using {card_sel!r}")
                    else:
                        print("    No cards found on this page — skipping")
                        continue

                cfg_resolved["_resolved_card_sel"] = card_sel
                current_listing = page.url
                page_num = 1

                # Paginate through current listing URL
                while True:
                    cards = await page.query_selector_all(card_sel)

                    if not cards:
                        print(f"    No cards on page {page_num} — stopping pagination")
                        break

                    print(f"    Page {page_num}: {len(cards)} cards "
                          f"(total so far: {len(collected)})")

                    # Build stubs from cards (before navigating away)
                    stubs = []
                    for card_el in cards:
                        stub = await extract_card_stub(card_el, cfg_resolved, base_url)
                        stubs.append(stub)

                    detail_type = cfg.get("detail_type", "navigate")

                    for idx, stub in enumerate(stubs):
                        title = stub.get("title") or stub.get("url") or f"product-{len(collected)+1}"
                        print(f"    [{len(collected)+1}] {title[:80]}")

                        # Skip already-collected detail URLs
                        if stub.get("url") and stub["url"] in seen_detail_urls:
                            continue
                        if stub.get("url"):
                            seen_detail_urls.add(stub["url"])

                        detail: dict = {}

                        if detail_type == "none":
                            # No separate detail page — listing card is all we get
                            pass

                        elif detail_type in ("navigate", "ajax"):
                            for attempt in range(1, 3):
                                try:
                                    pre_url = page.url
                                    if page.url != current_listing:
                                        await load_page(page, current_listing, cfg)

                                    cards_fresh = await page.query_selector_all(card_sel)
                                    if idx >= len(cards_fresh):
                                        break

                                    link_sel = cfg.get("card_link_selector")
                                    link_el = await cards_fresh[idx].query_selector(
                                        link_sel if link_sel else "a"
                                    )
                                    if not link_el:
                                        break

                                    href = await link_el.get_attribute("href")
                                    if not href:
                                        break

                                    target = abs_url(base_url, href)
                                    if not target:
                                        break

                                    # If link goes off-site, navigate directly
                                    if not same_origin(target, base_url):
                                        await load_page(page, target, cfg)
                                    else:
                                        await link_el.click()
                                        await wait_for_detail(page, cfg, pre_url)

                                    detail = await extract_detail_generic(page, cfg, base_url)
                                    detail["detail_url"] = page.url

                                    # Return to listing
                                    if page.url != current_listing:
                                        try:
                                            await page.go_back(
                                                wait_until=cfg.get("wait_until", "domcontentloaded"),
                                                timeout=30_000,
                                            )
                                            await asyncio.sleep(cfg.get("settle_ms", 1500) / 1000)
                                            current_listing = page.url
                                        except Exception:
                                            await load_page(page, current_listing, cfg)
                                            current_listing = page.url
                                    break

                                except Exception as exc:
                                    if attempt == 2:
                                        detail["detail_error"] = str(exc)
                                    else:
                                        await asyncio.sleep(2)
                                        await load_page(page, current_listing, cfg)

                        stub.update(detail)
                        collected.append(stub)

                    # Check pagination
                    next_sel = cfg.get("next_page_selector")
                    if not next_sel:
                        break
                    try:
                        next_btn = await page.query_selector(next_sel)
                    except Exception:
                        next_btn = None
                    if not next_btn:
                        break

                    print(f"    → Page {page_num + 1} …")
                    await next_btn.click()
                    try:
                        await page.wait_for_load_state(
                            cfg.get("wait_until", "domcontentloaded"), timeout=30_000
                        )
                    except PlaywrightTimeout:
                        pass
                    await asyncio.sleep(cfg.get("settle_ms", 1500) / 1000)
                    current_listing = page.url
                    page_num += 1

    except Exception as exc:
        print(f"  [error] site scrape failed: {exc}")

    finally:
        await ctx.close()

    if collected:
        _save(collected, output_file)
    else:
        print(f"  [warn] No products collected for {site_key}")

    print(f"  Done {site_key}: {len(collected)} products")
    return collected


# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    # Decide which sites to run
    if ACTIVE_SITE == "all":
        sites_to_run = list(SITE_CONFIGS.keys())
    else:
        if ACTIVE_SITE not in SITE_CONFIGS:
            print(f"ERROR: '{ACTIVE_SITE}' not in SITE_CONFIGS. "
                  f"Available: {list(SITE_CONFIGS.keys())}")
            return
        sites_to_run = [ACTIVE_SITE]

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print(f"\nUniversal Defence Scraper")
    print(f"Sites to scrape: {len(sites_to_run)}")
    print(f"Output directory: {OUTPUT_DIR}/\n")

    grand_total = 0

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
            ],
        )

        for site_key in sites_to_run:
            cfg = dict(SITE_CONFIGS[site_key])
            try:
                products = await scrape_site(site_key, cfg, browser)
                grand_total += len(products)
            except Exception as exc:
                print(f"  [FATAL] {site_key}: {exc}")

        await browser.close()

    print(f"\n{'='*60}")
    print(f"ALL DONE. Grand total products: {grand_total}")
    print(f"Files saved to: {OUTPUT_DIR}/")
    print(f"{'='*60}")


if __name__ == "__main__":
    asyncio.run(main())
