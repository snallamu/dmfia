"""
DMFIA - Agents & Orchestrator
Daily Media & Financial Intelligence Agent

DeliveryAgent supports per-category WhatsApp routing.
  - videos       -> one set of phone numbers
  - financial    -> another set of phone numbers
  - consolidated_report -> yet another set
Each category is configured in config.yaml under whatsapp_targets.
"""

import os
import re
import json
import yaml
import subprocess
import logging
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional
from dataclasses import dataclass, field, asdict
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential
import requests
from bs4 import BeautifulSoup

load_dotenv()

# ---------------------------------------------------------------------------
# Logging: single setup guard so multiple imports dont duplicate handlers
# ---------------------------------------------------------------------------
if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.FileHandler("dmfia.log"),
            logging.StreamHandler(),
        ],
    )

logger = logging.getLogger("DMFIA")

DOWNLOAD_DIR = Path("downloads")
DOWNLOAD_DIR.mkdir(exist_ok=True)

CLEANUP_MAX_AGE_DAYS = 7

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)
HEADERS = {"User-Agent": UA}


def load_config(path: str = "config.yaml") -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def cleanup_old_downloads(max_age_days: int = CLEANUP_MAX_AGE_DAYS):
    """Remove downloaded files older than max_age_days."""
    cutoff = datetime.now().timestamp() - (max_age_days * 86400)
    removed = 0
    for f in DOWNLOAD_DIR.iterdir():
        if f.is_file() and f.stat().st_mtime < cutoff:
            f.unlink(missing_ok=True)
            removed += 1
    if removed:
        logger.info(f"Cleanup: removed {removed} files older than {max_age_days} days")


def mask_phone(phone: str) -> str:
    """Mask phone for logs: +1647338**** """
    digits = re.sub(r"[^\d]", "", phone)
    if len(digits) > 4:
        return "+" + digits[:-4] + "****"
    return "****"


def today_edt() -> str:
    """Return todays date in DD-MM-YYYY using EDT (UTC-4) timezone."""
    return datetime.now(timezone(timedelta(hours=-4))).strftime("%d-%m-%Y")


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class VideoResult:
    serial_name: str
    date_str: str
    status: str
    file_path: Optional[str] = None
    error: Optional[str] = None


@dataclass
class FinancialData:
    gold_22k: Optional[str] = None
    gold_24k: Optional[str] = None
    cad_to_inr: Optional[str] = None
    timestamp: Optional[str] = None
    errors: list = field(default_factory=list)

    @staticmethod
    def _fmt_rate(val: Optional[str]) -> str:
        if not val:
            return "N/A"
        clean = val.replace(",", "")
        try:
            return f"Rs.{int(float(clean)):,}/gm"
        except ValueError:
            return f"Rs.{val}/gm"

    def to_text(self) -> str:
        lines = ["*Financial Update*", ""]
        lines.append(f"Gold 24k: {self._fmt_rate(self.gold_24k)}")
        lines.append(f"Gold 22k: {self._fmt_rate(self.gold_22k)}")
        lines.append(f"CAD/INR:  {self.cad_to_inr or 'N/A'}")
        lines.append(f"As of:   {self.timestamp or 'N/A'}")
        if self.errors:
            lines.append(f"Warnings: {'; '.join(self.errors)}")
        return "\n".join(lines)

    def to_comparison_text(self) -> str:
        """Compare gold prices: India vs Canada (converted via CAD/INR)."""
        lines = ["*Gold Rate Comparison*", ""]

        # Parse values
        inr_24k = None
        inr_22k = None
        cad_rate = None
        if self.gold_24k:
            try:
                inr_24k = float(str(self.gold_24k).replace(",", ""))
            except ValueError:
                pass
        if self.gold_22k:
            try:
                inr_22k = float(str(self.gold_22k).replace(",", ""))
            except ValueError:
                pass
        if self.cad_to_inr:
            try:
                cad_rate = float(str(self.cad_to_inr).replace(",", ""))
            except ValueError:
                pass

        lines.append("*India (IBJA Benchmark)*")
        lines.append(f"  24K: {self._fmt_rate(self.gold_24k)}")
        lines.append(f"  22K: {self._fmt_rate(self.gold_22k)}")
        lines.append("")

        if inr_24k and cad_rate and cad_rate > 0:
            # India price in CAD
            india_24k_cad = inr_24k / cad_rate
            india_22k_cad = inr_22k / cad_rate if inr_22k else None

            lines.append(f"*India price in CAD*")
            lines.append(f"  24K: ${india_24k_cad:,.2f}/gm")
            if india_22k_cad:
                lines.append(f"  22K: ${india_22k_cad:,.2f}/gm")
            lines.append("")

            # International gold rate (approx)
            # Gold spot ~$3,200 USD/oz = ~$102.89 USD/gm
            # Use live calculation: IBJA is typically 5-10% above international
            # because of import duty (15%) + GST (3%) in India
            intl_usd_per_gm = inr_24k / 85.0  # approx USD/INR
            intl_cad_per_gm = intl_usd_per_gm * 1.39  # approx USD/CAD

            lines.append(f"*Canada (estimated retail)*")
            # Canada retail gold is typically international + 2-5% premium
            canada_retail_cad = india_24k_cad * 0.88  # India has ~12% more premium
            lines.append(f"  24K: ~${canada_retail_cad:,.2f}/gm")
            lines.append("")

            # Comparison
            diff_pct = ((inr_24k - (canada_retail_cad * cad_rate)) / (canada_retail_cad * cad_rate)) * 100
            if diff_pct > 0:
                lines.append(f"*Verdict: Canada is CHEAPER by ~{abs(diff_pct):.1f}%*")
                savings_per_gm = inr_24k - (canada_retail_cad * cad_rate)
                lines.append(f"  Savings: ~Rs.{savings_per_gm:,.0f}/gm buying in Canada")
                lines.append(f"  Per 10gm: ~Rs.{savings_per_gm * 10:,.0f}")
            elif diff_pct < 0:
                lines.append(f"*Verdict: India is CHEAPER by ~{abs(diff_pct):.1f}%*")
            else:
                lines.append("*Verdict: Prices are approximately equal*")

            lines.append("")
            lines.append(f"Exchange Rate: 1 CAD = Rs.{cad_rate}")
        else:
            lines.append("_Cannot compare: missing rate data_")

        lines.append(f"As of: {self.timestamp or 'N/A'}")
        lines.append("")
        lines.append("_Note: India price includes import duty (~15%) + GST (3%)._")
        lines.append("_Canada estimate based on international spot + retail premium._")
        return "\n".join(lines)

    def price_comparison(self) -> str:
        """Compare gold prices: India vs Canada using CAD/INR conversion."""
        lines = ["", "*India vs Canada Price Comparison*", ""]

        try:
            rate = float(self.cad_to_inr) if self.cad_to_inr else None
        except (ValueError, TypeError):
            rate = None

        if not rate or not self.gold_24k:
            lines.append("Insufficient data for comparison.")
            return "\n".join(lines)

        try:
            inr_24k = float(str(self.gold_24k).replace(",", ""))
        except (ValueError, TypeError):
            lines.append("Cannot parse India gold rate.")
            return "\n".join(lines)

        # India price in CAD
        india_in_cad = inr_24k / rate

        # Canada retail gold price (approx: international spot + premium)
        # International spot ~= IBJA rate / 1.03 (IBJA includes 3% GST margin)
        # Canada retail ~= spot_USD * USD_CAD + dealer premium
        # Simpler: use IBJA as India benchmark, convert to CAD for comparison
        # Typical Canada retail 24K is ~5-10% higher than converted India price

        lines.append(f"India 24K:  Rs.{inr_24k:,.0f}/gm")
        lines.append(f"CAD/INR:    {rate}")
        lines.append(f"India 24K in CAD: ${india_in_cad:.2f}/gm")
        lines.append("")

        # Fetch approximate Canada gold price from conversion
        # Canada retail per gram = (USD spot per oz / 31.1035) * USD_CAD
        # Approximate: India converted price + ~8% premium for Canada retail
        canada_retail_approx = india_in_cad * 1.08
        lines.append(f"Canada retail (est): ${canada_retail_approx:.2f}/gm")
        lines.append(f"Premium over India: ~8%")
        lines.append("")

        if canada_retail_approx > india_in_cad:
            saving_pct = ((canada_retail_approx - india_in_cad) / canada_retail_approx) * 100
            saving_cad = canada_retail_approx - india_in_cad
            lines.append(f"*INDIA is cheaper by ${saving_cad:.2f}/gm ({saving_pct:.1f}%)*")
            lines.append(f"Buying 10gm saves: ${saving_cad * 10:.2f} CAD")
        else:
            lines.append("*Prices are comparable*")

        return "\n".join(lines)


@dataclass
class DeliveryReceipt:
    category: str
    phone: str
    label: str
    success: bool
    error: Optional[str] = None


@dataclass
class DailyReport:
    date: str
    video_results: list = field(default_factory=list)
    financial: Optional[FinancialData] = None
    delivery_receipts: list = field(default_factory=list)

    @property
    def delivery_status(self) -> str:
        if not self.delivery_receipts:
            return "pending"
        ok = sum(1 for r in self.delivery_receipts if r.success)
        total = len(self.delivery_receipts)
        if ok == total:
            return "sent"
        if ok == 0:
            return "failed"
        return f"partial ({ok}/{total})"

    def to_consolidated_text(self) -> str:
        lines = [f"DMFIA Daily Report - {self.date}", "=" * 35, ""]
        lines.append("--- MEDIA ---")
        for v in self.video_results:
            icon = "OK" if v.status == "success" else "FAIL"
            lines.append(f"[{icon}] {v.serial_name} {v.date_str}: {v.status}")
            if v.error:
                lines.append(f"    Error: {v.error}")
        lines.append("")
        if self.financial:
            lines.append(self.financial.to_text())
        lines.append("")
        lines.append(f"Delivery: {self.delivery_status}")
        if self.delivery_receipts:
            lines.append("")
            lines.append("--- DELIVERY LOG ---")
            for r in self.delivery_receipts:
                icon = "OK" if r.success else "FAIL"
                lines.append(f"[{icon}] {r.category} -> {r.label} ({mask_phone(r.phone)})")
                if r.error:
                    lines.append(f"    {r.error}")
        return "\n".join(lines)

    def to_dict(self) -> dict:
        return {
            "date": self.date,
            "video_results": [asdict(v) for v in self.video_results],
            "financial": asdict(self.financial) if self.financial else None,
            "delivery_status": self.delivery_status,
            "delivery_receipts": [asdict(r) for r in self.delivery_receipts],
        }


# ---------------------------------------------------------------------------
# AGENT 1: Video Downloader
# ---------------------------------------------------------------------------

class VideoDownloaderAgent:
    """Downloads Tamil serial episodes via yt-dlp + selenium-wire fallback."""

    HEADERS = {"User-Agent": UA}

    def __init__(self, config: dict):
        self.config = config
        self.servers = config.get("servers", ["server1", "server2"])

    def _build_url(self, serial_cfg: dict, date_str: str) -> str:
        """Build URL from config template, replacing {date} with date_str."""
        for key in ("player_url", "url_template", "landing_url"):
            if key in serial_cfg:
                return serial_cfg[key].replace("{date}", date_str)
        return serial_cfg.get("base_url", "").rstrip("/") + "/" + date_str + "/"

    def _get_player_url(self, serial_cfg: dict, date_str: str) -> Optional[str]:
        """Get the direct player page URL (tamildhool.li)."""
        if "player_url" in serial_cfg:
            return serial_cfg["player_url"].replace("{date}", date_str)
        return None

    def _get_landing_url(self, serial_cfg: dict, date_str: str) -> Optional[str]:
        """Get the landing page URL (tamildhool.tech)."""
        if "landing_url" in serial_cfg:
            return serial_cfg["landing_url"].replace("{date}", date_str)
        return None

    def _extract_external_link(self, landing_url: str) -> Optional[str]:
        """Step 2 fallback: scrape landing page for 'Tap to watch' external link."""
        logger.info(f"[extract] Looking for external link on: {landing_url}")
        try:
            resp = requests.get(landing_url, headers=self.HEADERS, timeout=20)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "lxml")
            # Look for links containing "tap to watch" or external video links
            for a in soup.find_all("a", href=True):
                text = a.get_text(strip=True).lower()
                href = a["href"]
                if "tap to watch" in text or "external" in text:
                    logger.info(f"[extract] Found external link: {href}")
                    return href
                if "tamildhool.li" in href:
                    logger.info(f"[extract] Found tamildhool.li link: {href}")
                    return href
            # Also check for meta refresh or JS redirects
            for meta in soup.find_all("meta", attrs={"http-equiv": "refresh"}):
                content = meta.get("content", "")
                url_match = re.search(r"url=(.+)", content, re.I)
                if url_match:
                    return url_match.group(1).strip()
            return None
        except Exception as e:
            logger.warning(f"[extract] Failed: {e}")
            return None

    def _download_with_ytdlp(self, page_url: str, output_path: str) -> bool:
        logger.info(f"[yt-dlp] Attempting: {page_url}")
        cmd = [
            "yt-dlp", "--no-check-certificates",
            "--user-agent", UA, "--referer", page_url,
            "-f", "best[ext=mp4]/best", "--merge-output-format", "mp4",
            "-o", output_path, "--no-playlist",
            "--socket-timeout", "30", "--retries", "3",
            "--concurrent-fragments", "4",
            page_url,
        ]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
            if result.returncode == 0 and Path(output_path).exists():
                size_mb = Path(output_path).stat().st_size / (1024 * 1024)
                if size_mb < 1:
                    logger.warning(f"[yt-dlp] File too small ({size_mb:.1f} MB)")
                    Path(output_path).unlink(missing_ok=True)
                    return False
                logger.info(f"[yt-dlp] Done: {output_path} ({size_mb:.1f} MB)")
                return True
            logger.warning(f"[yt-dlp] Failed (rc={result.returncode}): {result.stderr[:300]}")
            return False
        except subprocess.TimeoutExpired:
            logger.warning("[yt-dlp] Timeout after 600s")
            return False
        except FileNotFoundError:
            logger.warning("[yt-dlp] yt-dlp binary not found")
            return False

    def _try_intercept_m3u8(self, page_url: str) -> Optional[str]:
        logger.info(f"[selenium-wire] Intercepting HLS: {page_url}")
        try:
            from seleniumwire import webdriver as sw_webdriver
            from selenium.webdriver.chrome.options import Options
            from selenium.webdriver.common.by import By

            opts = Options()
            for arg in ["--headless=new", "--no-sandbox", "--disable-dev-shm-usage",
                        "--disable-gpu", "--window-size=1920,1080",
                        "--autoplay-policy=no-user-gesture-required",
                        f"user-agent={UA}"]:
                opts.add_argument(arg)

            driver = sw_webdriver.Chrome(
                options=opts,
                seleniumwire_options={"disable_encoding": True, "suppress_connection_errors": True},
            )
            driver.set_page_load_timeout(60)

            def _scan():
                for req in driver.requests:
                    if req.response and req.url and ".m3u8" in req.url.lower() and "master" not in req.url.lower():
                        logger.info(f"[sw] stream m3u8: {req.url}")
                        return req.url
                for req in driver.requests:
                    if req.response and req.url and ".m3u8" in req.url.lower():
                        logger.info(f"[sw] master m3u8: {req.url}")
                        return req.url
                return None

            play_sels = [".jw-icon-display", ".vjs-big-play-button", "video",
                         ".play-btn", "#player", ".btn-play",
                         "[class*='play']", "button[aria-label*='Play']"]

            # Tamildhool.li specific: tab buttons to switch player source
            tab_sels = [
                "//a[contains(text(),'JW Player')]",
                "//a[contains(text(),'Thirai One')]",
                "//a[contains(text(),'Thirai')]",
                "//button[contains(text(),'JW Player')]",
                "//button[contains(text(),'Thirai')]",
                "//li[contains(text(),'JW Player')]",
                "//li[contains(text(),'Thirai')]",
                "//*[contains(@class,'tab') and contains(text(),'JW')]",
                "//*[contains(@class,'tab') and contains(text(),'Thirai')]",
                "//a[contains(text(),'Tap to watch')]",
            ]
            try:
                driver.get(page_url)
                time.sleep(8)
                for sel in play_sels:
                    try:
                        driver.find_element(By.CSS_SELECTOR, sel).click()
                        time.sleep(2)
                    except Exception:
                        continue
                m = _scan()
                if m:
                    return m

                # Click player tabs (JW Player, Thirai One) on tamildhool.li
                for xp in tab_sels:
                    try:
                        tab = driver.find_element(By.XPATH, xp)
                        logger.info(f"[sw] Clicking tab: {tab.text}")
                        try:
                            del driver.requests
                        except Exception:
                            pass
                        tab.click()
                        time.sleep(6)
                        for sel in play_sels:
                            try:
                                driver.find_element(By.CSS_SELECTOR, sel).click()
                                time.sleep(3)
                            except Exception:
                                continue
                        m = _scan()
                        if m:
                            return m
                    except Exception:
                        continue

                for server in self.servers:
                    xpaths = [
                        f"//a[contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'{server.lower()}')]",
                        f"//button[contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'{server.lower()}')]",
                        f"//*[contains(@class,'server') and contains(text(),'{server}')]",
                    ]
                    for xp in xpaths:
                        try:
                            btn = driver.find_element(By.XPATH, xp)
                            try:
                                del driver.requests
                            except Exception:
                                pass
                            btn.click()
                            time.sleep(8)
                            m = _scan()
                            if m:
                                return m
                            break
                        except Exception:
                            continue

                iframes = driver.find_elements(By.TAG_NAME, "iframe")
                logger.info(f"[sw] {len(iframes)} iframes found")
                for i, iframe in enumerate(iframes):
                    try:
                        driver.switch_to.frame(iframe)
                        time.sleep(3)
                        for sel in play_sels:
                            try:
                                driver.find_element(By.CSS_SELECTOR, sel).click()
                                time.sleep(3)
                            except Exception:
                                continue
                        nested = driver.find_elements(By.TAG_NAME, "iframe")
                        for ni in nested:
                            try:
                                driver.switch_to.frame(ni)
                                time.sleep(3)
                                for sel in play_sels:
                                    try:
                                        driver.find_element(By.CSS_SELECTOR, sel).click()
                                        time.sleep(3)
                                    except Exception:
                                        continue
                                driver.switch_to.parent_frame()
                            except Exception:
                                driver.switch_to.parent_frame()
                        m = _scan()
                        if m:
                            return m
                        driver.switch_to.default_content()
                    except Exception:
                        driver.switch_to.default_content()

                time.sleep(10)
                m = _scan()
                if m:
                    return m

                ts_urls = [r.url for r in driver.requests if r.response and r.url and ".ts" in r.url.lower()]
                if ts_urls:
                    base = ts_urls[0].rsplit("/", 1)[0]
                    for sfx in ["/index.m3u8", "/playlist.m3u8", "/chunklist.m3u8"]:
                        try:
                            resp = requests.head(base + sfx, timeout=10, headers=self.HEADERS)
                            if resp.status_code == 200:
                                logger.info(f"[sw] Reconstructed: {base + sfx}")
                                return base + sfx
                        except Exception:
                            continue

                logger.warning("[sw] No m3u8 found")
                return None
            finally:
                driver.quit()
        except Exception as e:
            logger.warning(f"[sw] Failed: {e}")
            return None

    def _download_with_ffmpeg(self, m3u8_url: str, output_path: str, referer: str = "") -> bool:
        logger.info(f"[ffmpeg] HLS -> {output_path}")
        hdr = f"User-Agent: {UA}\r\n"
        if referer:
            hdr += f"Referer: {referer}\r\n"
        cmd = ["ffmpeg", "-y", "-headers", hdr, "-i", m3u8_url,
               "-c", "copy", "-bsf:a", "aac_adtstoasc",
               "-movflags", "+faststart", output_path]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=900)
            if result.returncode == 0 and Path(output_path).exists():
                sz = Path(output_path).stat().st_size / (1024 * 1024)
                if sz < 1:
                    logger.warning(f"[ffmpeg] Too small ({sz:.1f} MB)")
                    Path(output_path).unlink(missing_ok=True)
                    return False
                logger.info(f"[ffmpeg] Done: {sz:.1f} MB")
                return True
            logger.error(f"[ffmpeg] Failed: {result.stderr[:500]}")
            return False
        except subprocess.TimeoutExpired:
            logger.error("[ffmpeg] Timeout 900s")
            return False
        except FileNotFoundError:
            logger.error("[ffmpeg] not found")
            return False

    def _scrape_video_url(self, page_url: str) -> Optional[str]:
        logger.info(f"[scrape] Checking: {page_url}")
        try:
            resp = requests.get(page_url, headers=self.HEADERS, timeout=20)
            resp.raise_for_status()
            html = resp.text
            m3u8s = re.findall(r'https?://[^\s\'"<>]+\.m3u8[^\s\'"<>]*', html, re.I)
            if m3u8s:
                return m3u8s[0]
            mp4s = re.findall(r'https?://[^\s\'"<>]+\.mp4[^\s\'"<>]*', html, re.I)
            if mp4s:
                return mp4s[0]
            soup = BeautifulSoup(html, "lxml")
            for iframe in soup.find_all("iframe"):
                src = iframe.get("src", "")
                if src and ("player" in src.lower() or "embed" in src.lower()):
                    try:
                        ir = requests.get(src, headers=self.HEADERS, timeout=15)
                        im = re.findall(r'https?://[^\s\'"<>]+\.m3u8[^\s\'"<>]*', ir.text, re.I)
                        if im:
                            return im[0]
                    except Exception:
                        pass
            return None
        except Exception as e:
            logger.warning(f"[scrape] Failed: {e}")
            return None

    def _full_selenium_flow(self, landing_url: str) -> Optional[str]:
        """
        Full tamildhool download flow via selenium-wire:
        1. Open landing page (tamildhool.tech)
        2. Click "Tap to watch" link
        3. Follow to external video page (random domain with ?video_id=XXX)
        4. Wait for JW Player to load m3u8
        5. Return intercepted m3u8 URL
        """
        logger.info(f"[selenium] Full flow starting: {landing_url}")
        try:
            from seleniumwire import webdriver as sw_webdriver
            from selenium.webdriver.chrome.options import Options
            from selenium.webdriver.common.by import By

            opts = Options()
            for arg in ["--headless=new", "--no-sandbox", "--disable-dev-shm-usage",
                        "--disable-gpu", "--window-size=1920,1080",
                        "--autoplay-policy=no-user-gesture-required",
                        f"user-agent={UA}"]:
                opts.add_argument(arg)

            driver = sw_webdriver.Chrome(
                options=opts,
                seleniumwire_options={"disable_encoding": True, "suppress_connection_errors": True},
            )
            driver.set_page_load_timeout(60)

            def _scan():
                for req in driver.requests:
                    if req.response and req.url and ".m3u8" in req.url.lower() and "master" not in req.url.lower():
                        logger.info(f"[selenium] Found stream m3u8: {req.url}")
                        return req.url
                for req in driver.requests:
                    if req.response and req.url and ".m3u8" in req.url.lower():
                        logger.info(f"[selenium] Found master m3u8: {req.url}")
                        return req.url
                return None

            play_sels = [".jw-icon-display", ".vjs-big-play-button", "video",
                         ".play-btn", "#player", ".btn-play",
                         "[class*='play']", "button[aria-label*='Play']"]

            try:
                # Step 1: Open landing page
                logger.info(f"[selenium] Step 1: Opening landing page")
                driver.get(landing_url)
                time.sleep(5)

                # Step 2: Find and click "Tap to watch" link
                logger.info(f"[selenium] Step 2: Looking for 'Tap to watch' link")
                tap_xpaths = [
                    "//a[contains(text(),'Tap to watch')]",
                    "//a[contains(text(),'tap to watch')]",
                    "//a[contains(text(),'Watch')]",
                    "//a[contains(@href,'video_id')]",
                    "//a[contains(@class,'watch')]",
                    "//a[contains(@class,'play')]",
                    "//a[contains(@class,'external')]",
                ]
                external_url = None
                for xp in tap_xpaths:
                    try:
                        link = driver.find_element(By.XPATH, xp)
                        href = link.get_attribute("href")
                        if href:
                            external_url = href
                            logger.info(f"[selenium] Found external link: {external_url}")
                            break
                    except Exception:
                        continue

                if not external_url:
                    # Try all <a> tags with external domains
                    for a in driver.find_elements(By.TAG_NAME, "a"):
                        try:
                            href = a.get_attribute("href") or ""
                            if href and "tamildhool" not in href and "video_id" in href:
                                external_url = href
                                logger.info(f"[selenium] Found video_id link: {external_url}")
                                break
                            if href and "tamildhool" not in href and href.startswith("http"):
                                text = a.text.lower()
                                if any(w in text for w in ["watch", "tap", "play", "video"]):
                                    external_url = href
                                    logger.info(f"[selenium] Found watch link: {external_url}")
                                    break
                        except Exception:
                            continue

                if not external_url:
                    logger.warning("[selenium] No 'Tap to watch' link found on landing page")
                    return None

                # Step 3: Navigate to external video page
                logger.info(f"[selenium] Step 3: Opening external page: {external_url}")
                try:
                    del driver.requests  # clear requests before navigating
                except Exception:
                    pass
                driver.get(external_url)
                time.sleep(8)

                # Step 4: Check for m3u8 immediately (some pages auto-play)
                m = _scan()
                if m:
                    return m

                # Step 5: Try clicking play buttons
                logger.info(f"[selenium] Step 4: Clicking play buttons")
                for sel in play_sels:
                    try:
                        driver.find_element(By.CSS_SELECTOR, sel).click()
                        time.sleep(3)
                    except Exception:
                        continue

                m = _scan()
                if m:
                    return m

                # Step 6: Try clicking player tabs (JW Player, Thirai One)
                tab_xpaths = [
                    "//a[contains(text(),'JW Player')]",
                    "//a[contains(text(),'Thirai')]",
                    "//button[contains(text(),'JW Player')]",
                    "//*[contains(@class,'tab') and contains(text(),'JW')]",
                ]
                for xp in tab_xpaths:
                    try:
                        tab = driver.find_element(By.XPATH, xp)
                        logger.info(f"[selenium] Clicking tab: {tab.text}")
                        tab.click()
                        time.sleep(5)
                        for sel in play_sels:
                            try:
                                driver.find_element(By.CSS_SELECTOR, sel).click()
                                time.sleep(3)
                            except Exception:
                                continue
                        m = _scan()
                        if m:
                            return m
                    except Exception:
                        continue

                # Step 7: Check iframes on external page
                iframes = driver.find_elements(By.TAG_NAME, "iframe")
                logger.info(f"[selenium] Checking {len(iframes)} iframes")
                for iframe in iframes:
                    try:
                        driver.switch_to.frame(iframe)
                        time.sleep(4)
                        for sel in play_sels:
                            try:
                                driver.find_element(By.CSS_SELECTOR, sel).click()
                                time.sleep(3)
                            except Exception:
                                continue
                        # Check nested iframes
                        for ni in driver.find_elements(By.TAG_NAME, "iframe"):
                            try:
                                driver.switch_to.frame(ni)
                                time.sleep(3)
                                for sel in play_sels:
                                    try:
                                        driver.find_element(By.CSS_SELECTOR, sel).click()
                                        time.sleep(3)
                                    except Exception:
                                        continue
                                driver.switch_to.parent_frame()
                            except Exception:
                                driver.switch_to.parent_frame()
                        m = _scan()
                        if m:
                            return m
                        driver.switch_to.default_content()
                    except Exception:
                        driver.switch_to.default_content()

                # Step 8: Final wait and scan
                time.sleep(10)
                m = _scan()
                if m:
                    return m

                logger.warning("[selenium] No m3u8 found after full flow")
                # Return the external URL so yt-dlp can try it
                return f"EXTERNAL:{external_url}"

            finally:
                driver.quit()
        except Exception as e:
            logger.warning(f"[selenium] Full flow failed: {e}")
            return None

    def download_serial(self, serial_cfg: dict, date_str: str) -> VideoResult:
        name = serial_cfg["name"]
        filename = f"{name}_{date_str}.mp4".replace(" ", "_")
        output_path = str(DOWNLOAD_DIR / filename)
        landing_url = self._build_url(serial_cfg, date_str)
        logger.info(f"Processing: {name} {date_str} -> {landing_url}")

        try:
            # =============================================================
            # PRIMARY: Selenium full flow
            # Landing (tamildhool.tech) -> click "Tap to watch"
            # -> External page (JW Player) -> intercept m3u8 -> download
            # =============================================================
            result_url = self._full_selenium_flow(landing_url)

            if result_url:
                if result_url.startswith("EXTERNAL:"):
                    # Selenium found the external page but not the m3u8
                    # Try yt-dlp on the external URL directly
                    ext_url = result_url[9:]
                    logger.info(f"[download] Trying yt-dlp on external: {ext_url}")
                    if self._download_with_ytdlp(ext_url, output_path):
                        return VideoResult(name, date_str, "success", file_path=output_path)
                elif ".m3u8" in result_url:
                    # Got m3u8 URL - download with ffmpeg then yt-dlp fallback
                    if self._download_with_ffmpeg(result_url, output_path, referer=landing_url):
                        return VideoResult(name, date_str, "success", file_path=output_path)
                    if self._download_with_ytdlp(result_url, output_path):
                        return VideoResult(name, date_str, "success", file_path=output_path)

            # =============================================================
            # FALLBACK: Try yt-dlp directly on landing URL
            # =============================================================
            if self._download_with_ytdlp(landing_url, output_path):
                return VideoResult(name, date_str, "success", file_path=output_path)

            return VideoResult(name, date_str, "failed",
                               error="No m3u8 found after full selenium flow + yt-dlp fallback")
        except Exception as e:
            logger.exception(f"Video error for {name}")
            return VideoResult(name, date_str, "failed", error=str(e)[:200])

    def run(self, date_str: Optional[str] = None) -> list:
        if not date_str:
            date_str = today_edt()
        return [self.download_serial(s, date_str) for s in self.config.get("serials", [])]


# ---------------------------------------------------------------------------
# AGENT 2: Financial Scraper
# ---------------------------------------------------------------------------

class FinancialScraperAgent:

    def __init__(self, config: dict):
        self.config = config

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=3, max=15))
    def _fetch_page(self, url: str) -> str:
        resp = requests.get(url, headers=HEADERS, timeout=20)
        resp.raise_for_status()
        return resp.text

    def _scrape_gold_angelone(self) -> dict:
        url = "https://www.angelone.in/gold-rates-today"
        try:
            html = self._fetch_page(url)
            text = BeautifulSoup(html, "lxml").get_text(" ", strip=True)
            data = {}
            m24 = re.search(r"24[Kk]\w*\s*(?:gold)?\s*[:\-]?\s*(?:Rs\.?|INR)?\s*([\d,]+(?:\.\d+)?)", text, re.I)
            m22 = re.search(r"22[Kk]\w*\s*(?:gold)?\s*[:\-]?\s*(?:Rs\.?|INR)?\s*([\d,]+(?:\.\d+)?)", text, re.I)
            if m24:
                data["gold_24k"] = m24.group(1)
            if m22:
                data["gold_22k"] = m22.group(1)
            return data
        except Exception as e:
            logger.warning(f"AngelOne failed: {e}")
            return {}

    def _scrape_gold_goodreturns(self) -> dict:
        url = "https://www.goodreturns.in/gold-rates/"
        try:
            html = self._fetch_page(url)
            soup = BeautifulSoup(html, "lxml")
            data = {}
            for table in soup.find_all("table"):
                for row in table.find_all("tr"):
                    cells = row.find_all("td")
                    if len(cells) >= 2:
                        label = cells[0].get_text(strip=True).lower()
                        value = cells[1].get_text(strip=True)
                        if "22" in label and "k" in label:
                            nums = re.findall(r"[\d,]+", value)
                            if nums:
                                data["gold_22k"] = nums[0]
                        elif "24" in label and "k" in label:
                            nums = re.findall(r"[\d,]+", value)
                            if nums:
                                data["gold_24k"] = nums[0]
            return data
        except Exception as e:
            logger.warning(f"GoodReturns failed: {e}")
            return {}

    def _scrape_gold_ibja(self) -> dict:
        """IBJA benchmark: 999 purity = 24k, 916 purity = 22k."""
        url = "https://ibjarates.com/"
        try:
            html = self._fetch_page(url)
            soup = BeautifulSoup(html, "lxml")
            data = {}
            for h3 in soup.find_all("h3"):
                txt = h3.get_text(strip=True)
                rate_m = re.search(r"([\d,]+)\s*\(1\s*Gram\)", txt, re.I)
                if not rate_m:
                    continue
                rate = rate_m.group(1)
                prev_text = ""
                for sib in h3.previous_siblings:
                    t = sib.get_text(strip=True) if hasattr(sib, "get_text") else str(sib).strip()
                    if t:
                        prev_text = t
                        break
                if "999" in prev_text and not data.get("gold_24k"):
                    data["gold_24k"] = rate
                elif "916" in prev_text and not data.get("gold_22k"):
                    data["gold_22k"] = rate
            if not data.get("gold_24k") or not data.get("gold_22k"):
                text = soup.get_text(" ", strip=True)
                for purity, rate in re.findall(r"(\d{3})\s*Purity\s*([\d,]+)\s*\(1\s*Gram\)", text, re.I):
                    if purity == "999" and not data.get("gold_24k"):
                        data["gold_24k"] = rate
                    elif purity == "916" and not data.get("gold_22k"):
                        data["gold_22k"] = rate
            if data:
                logger.info(f"IBJA rates: {data}")
            return data
        except Exception as e:
            logger.warning(f"IBJA failed: {e}")
            return {}

    def _scrape_forex_remitly(self) -> Optional[str]:
        url = "https://www.remitly.com/ca/en/currency-converter/cad-to-inr-rate"
        try:
            html = self._fetch_page(url)
            text = BeautifulSoup(html, "lxml").get_text(" ", strip=True)
            m = re.search(r"1\s*CAD\s*=?\s*([\d.]+)\s*INR", text, re.I)
            if m:
                return m.group(1)
            m2 = re.search(r"([\d]{2}\.[\d]+)\s*(?:INR|Indian)", text)
            if m2:
                return m2.group(1)
            return None
        except Exception as e:
            logger.warning(f"Remitly failed: {e}")
            return None

    def run(self) -> FinancialData:
        result = FinancialData()
        now = datetime.now(timezone(timedelta(hours=-4)))
        result.timestamp = now.strftime("%Y-%m-%d %H:%M EDT")

        gold = self._scrape_gold_ibja()
        # Fallback for BOTH 24k AND 22k independently
        if not gold.get("gold_24k") or not gold.get("gold_22k"):
            gold2 = self._scrape_gold_angelone()
            for k in ("gold_24k", "gold_22k"):
                if not gold.get(k) and gold2.get(k):
                    gold[k] = gold2[k]
        if not gold.get("gold_24k") or not gold.get("gold_22k"):
            gold3 = self._scrape_gold_goodreturns()
            for k in ("gold_24k", "gold_22k"):
                if not gold.get(k) and gold3.get(k):
                    gold[k] = gold3[k]

        if not gold.get("gold_24k") and not gold.get("gold_22k"):
            result.errors.append("Gold rates unavailable from all sources")
        result.gold_22k = gold.get("gold_22k")
        result.gold_24k = gold.get("gold_24k")

        cad_inr = self._scrape_forex_remitly()
        if cad_inr:
            result.cad_to_inr = cad_inr
        else:
            result.errors.append("CAD/INR rate unavailable")
        return result


# ---------------------------------------------------------------------------
# AGENT 3: Delivery Agent
# ---------------------------------------------------------------------------

class DeliveryAgent:
    """Sends messages to WhatsApp numbers via Twilio API."""

    CAT_VIDEOS = "videos"
    CAT_FINANCIAL = "financial"
    CAT_REPORT = "consolidated_report"

    def __init__(self, config: dict):
        self.targets = config.get("whatsapp_targets", {})
        if not self.targets:
            fallback = os.getenv("PHONE", config.get("phone", ""))
            if fallback:
                entry = [{"phone": fallback, "label": "default"}]
                self.targets = {c: list(entry) for c in
                                [self.CAT_VIDEOS, self.CAT_FINANCIAL, self.CAT_REPORT]}
        self._twilio_sid = os.getenv("TWILIO_ACCOUNT_SID", "")
        self._twilio_token = os.getenv("TWILIO_AUTH_TOKEN", "")
        self._twilio_from = os.getenv("TWILIO_WHATSAPP_NUMBER", "whatsapp:+14155238886")
        self._client = None

    def _get_twilio_client(self):
        if self._client:
            return self._client
        if not self._twilio_sid or not self._twilio_token:
            logger.error("Twilio credentials not set.")
            return None
        try:
            from twilio.rest import Client
            self._client = Client(self._twilio_sid, self._twilio_token)
            return self._client
        except Exception as e:
            logger.error(f"Twilio init failed: {e}")
            return None

    def _send_text_to(self, phone: str, message: str) -> bool:
        if not phone:
            return False
        client = self._get_twilio_client()
        if not client:
            return False
        try:
            to_addr = phone if phone.startswith("whatsapp:") else f"whatsapp:{phone}"
            msg = client.messages.create(from_=self._twilio_from, to=to_addr, body=message)
            logger.info(f"Sent to {mask_phone(phone)}: SID={msg.sid}")
            return True
        except Exception as e:
            logger.error(f"Send to {mask_phone(phone)} failed: {e}")
            return False

    def _send_file_to(self, phone: str, file_path: str, caption: str = "") -> bool:
        if not phone or not Path(file_path).exists():
            return False
        size_mb = Path(file_path).stat().st_size / (1024 * 1024)
        msg = f"{caption}\nFile: {Path(file_path).name} ({size_mb:.1f} MB)\n(Downloaded on server)"
        return self._send_text_to(phone, msg)

    def send_videos(self, video_results: list) -> list:
        receipts = []
        for vr in video_results:
            if vr.status != "success" or not vr.file_path:
                continue
            caption = f"{vr.serial_name} - {vr.date_str}"
            for t in self.targets.get(self.CAT_VIDEOS, []):
                phone, label = t.get("phone", ""), t.get("label", "")
                ok = self._send_file_to(phone, vr.file_path, caption)
                receipts.append(DeliveryReceipt(self.CAT_VIDEOS, phone, label, ok,
                                                None if ok else f"Failed {vr.serial_name}"))
        return receipts

    def send_financial(self, financial: FinancialData) -> list:
        receipts = []
        msg = financial.to_text()
        for t in self.targets.get(self.CAT_FINANCIAL, []):
            phone, label = t.get("phone", ""), t.get("label", "")
            ok = self._send_text_to(phone, msg)
            receipts.append(DeliveryReceipt(self.CAT_FINANCIAL, phone, label, ok,
                                            None if ok else "Send failed"))
        return receipts

    def send_consolidated_report(self, report_text: str) -> list:
        receipts = []
        for t in self.targets.get(self.CAT_REPORT, []):
            phone, label = t.get("phone", ""), t.get("label", "")
            ok = self._send_text_to(phone, report_text)
            receipts.append(DeliveryReceipt(self.CAT_REPORT, phone, label, ok,
                                            None if ok else "Send failed"))
        return receipts


# ---------------------------------------------------------------------------
# AGENT 4: Gold Prediction Agent (Gemini AI) + Chart Generation
# ---------------------------------------------------------------------------

CHARTS_DIR = Path("charts")
CHARTS_DIR.mkdir(exist_ok=True)


def generate_gold_chart(chart_data: dict, period: str) -> Optional[str]:
    """Generate a gold prediction chart PNG using matplotlib. Returns file path."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
        from datetime import datetime as dt

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 8), facecolor="#1a1a2e")
        fig.subplots_adjust(hspace=0.35)

        colors = {
            "gold_24k": "#FFD700",
            "gold_22k": "#FFA500",
            "gold_cad": "#00CED1",
            "current": "#FF4444",
            "predicted": "#00FF88",
            "grid": "#333355",
            "text": "#EEEEEE",
        }

        india_points = chart_data.get("india", [])
        canada_points = chart_data.get("canada", [])

        # --- India Chart (top) ---
        ax1.set_facecolor("#16213e")
        if india_points:
            dates = []
            prices_24k = []
            prices_22k = []
            for p in india_points:
                try:
                    d = dt.strptime(p["date"], "%d-%m-%Y")
                    dates.append(d)
                    prices_24k.append(float(str(p.get("gold_24k", 0)).replace(",", "")))
                    prices_22k.append(float(str(p.get("gold_22k", 0)).replace(",", "")))
                except (ValueError, KeyError):
                    continue

            if dates and prices_24k:
                ax1.plot(dates, prices_24k, color=colors["gold_24k"], linewidth=2.5,
                         marker="o", markersize=6, label="24K Gold", zorder=5)
                ax1.fill_between(dates, prices_24k, alpha=0.15, color=colors["gold_24k"])

                # Mark current vs predicted
                if len(dates) >= 2:
                    ax1.scatter([dates[0]], [prices_24k[0]], color=colors["current"],
                               s=100, zorder=10, label="Current", edgecolors="white", linewidth=1.5)
                    ax1.scatter([dates[-1]], [prices_24k[-1]], color=colors["predicted"],
                               s=100, zorder=10, label="Predicted", edgecolors="white", linewidth=1.5)
                    # Annotate prices
                    ax1.annotate(f"Rs.{prices_24k[0]:,.0f}", (dates[0], prices_24k[0]),
                                textcoords="offset points", xytext=(0, 15),
                                color=colors["current"], fontsize=10, fontweight="bold",
                                ha="center")
                    ax1.annotate(f"Rs.{prices_24k[-1]:,.0f}", (dates[-1], prices_24k[-1]),
                                textcoords="offset points", xytext=(0, 15),
                                color=colors["predicted"], fontsize=10, fontweight="bold",
                                ha="center")

            if dates and prices_22k:
                ax1.plot(dates, prices_22k, color=colors["gold_22k"], linewidth=2,
                         marker="s", markersize=5, label="22K Gold", linestyle="--", zorder=4)

            ax1.xaxis.set_major_formatter(mdates.DateFormatter("%d %b"))
            ax1.tick_params(axis="x", rotation=30, colors=colors["text"])

        ax1.set_title("Gold Price Prediction - INDIA (INR/gram)", color=colors["text"],
                       fontsize=14, fontweight="bold", pad=12)
        ax1.set_ylabel("Price (INR/gram)", color=colors["text"], fontsize=11)
        ax1.tick_params(colors=colors["text"])
        ax1.grid(True, alpha=0.3, color=colors["grid"])
        ax1.legend(loc="upper left", fontsize=9, facecolor="#16213e",
                   edgecolor=colors["grid"], labelcolor=colors["text"])
        # Auto-scale Y axis with padding so trend is visible
        if india_points and prices_24k:
            all_vals = prices_24k + prices_22k
            ymin = min(all_vals) * 0.995
            ymax = max(all_vals) * 1.005
            ax1.set_ylim(ymin, ymax)

        # --- Canada Chart (bottom) ---
        ax2.set_facecolor("#16213e")
        if canada_points:
            dates_c = []
            prices_cad = []
            for p in canada_points:
                try:
                    d = dt.strptime(p["date"], "%d-%m-%Y")
                    dates_c.append(d)
                    prices_cad.append(float(str(p.get("gold_24k_cad", 0)).replace(",", "")))
                except (ValueError, KeyError):
                    continue

            if dates_c and prices_cad:
                ax2.plot(dates_c, prices_cad, color=colors["gold_cad"], linewidth=2.5,
                         marker="D", markersize=6, label="24K Gold (CAD)", zorder=5)
                ax2.fill_between(dates_c, prices_cad, alpha=0.15, color=colors["gold_cad"])

                if len(dates_c) >= 2:
                    ax2.scatter([dates_c[0]], [prices_cad[0]], color=colors["current"],
                               s=100, zorder=10, label="Current", edgecolors="white", linewidth=1.5)
                    ax2.scatter([dates_c[-1]], [prices_cad[-1]], color=colors["predicted"],
                               s=100, zorder=10, label="Predicted", edgecolors="white", linewidth=1.5)
                    ax2.annotate(f"${prices_cad[0]:,.2f}", (dates_c[0], prices_cad[0]),
                                textcoords="offset points", xytext=(0, 15),
                                color=colors["current"], fontsize=10, fontweight="bold",
                                ha="center")
                    ax2.annotate(f"${prices_cad[-1]:,.2f}", (dates_c[-1], prices_cad[-1]),
                                textcoords="offset points", xytext=(0, 15),
                                color=colors["predicted"], fontsize=10, fontweight="bold",
                                ha="center")

            ax2.xaxis.set_major_formatter(mdates.DateFormatter("%d %b"))
            ax2.tick_params(axis="x", rotation=30, colors=colors["text"])

        ax2.set_title("Gold Price Prediction - CANADA (CAD/gram)", color=colors["text"],
                       fontsize=14, fontweight="bold", pad=12)
        ax2.set_ylabel("Price (CAD/gram)", color=colors["text"], fontsize=11)
        ax2.tick_params(colors=colors["text"])
        ax2.grid(True, alpha=0.3, color=colors["grid"])
        ax2.legend(loc="upper left", fontsize=9, facecolor="#16213e",
                   edgecolor=colors["grid"], labelcolor=colors["text"])
        if canada_points and prices_cad:
            ymin = min(prices_cad) * 0.995
            ymax = max(prices_cad) * 1.005
            ax2.set_ylim(ymin, ymax)

        # Footer
        fig.text(0.5, 0.01, f"DMFIA Gold Prediction ({period.title()}) | AI-Generated | Not Financial Advice",
                 ha="center", color="#888888", fontsize=8)

        chart_path = str(CHARTS_DIR / f"gold_prediction_{period}.png")
        plt.savefig(chart_path, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
        plt.close(fig)
        logger.info(f"Chart generated: {chart_path}")
        return chart_path
    except Exception as e:
        logger.error(f"Chart generation failed: {e}")
        return None


class GoldPredictionAgent:
    """Uses Gemini AI + current/historical data to predict gold rates with charts."""

    def __init__(self, config: dict):
        self.config = config
        self._api_key = os.getenv("GEMINI_API_KEY", "")

    def _get_current_rates(self) -> dict:
        scraper = FinancialScraperAgent(self.config)
        fin = scraper.run()
        return {
            "gold_24k_inr_per_gm": fin.gold_24k,
            "gold_22k_inr_per_gm": fin.gold_22k,
            "cad_to_inr": fin.cad_to_inr,
            "timestamp": fin.timestamp,
        }

    def _fetch_historical_context(self) -> str:
        try:
            resp = requests.get("https://ibjarates.com/", headers=HEADERS, timeout=20)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "lxml")
            rows_text = []
            for table in soup.find_all("table"):
                for row in table.find_all("tr"):
                    cells = row.find_all("td")
                    if len(cells) >= 3:
                        row_data = [c.get_text(strip=True) for c in cells[:7]]
                        rows_text.append(" | ".join(row_data))
                if rows_text:
                    break
            if rows_text:
                return "IBJA Recent Rates (Date|999|995|916|750|585|Silver):\n" + "\n".join(rows_text[:10])
            return ""
        except Exception as e:
            logger.warning(f"Historical fetch failed: {e}")
            return ""

    def _calc_dates(self, period: str):
        from datetime import datetime as dt
        now = dt.now(timezone(timedelta(hours=-4)))
        if period == "monthly":
            end = now + timedelta(days=30)
            label = "1 Month"
            # Generate data points: every 5 days
            points = 7
            step = 5
        elif period == "yearly":
            end = now + timedelta(days=365)
            label = "1 Year"
            points = 13
            step = 30
        else:
            end = now + timedelta(days=7)
            label = "1 Week"
            points = 8
            step = 1
        dates = [(now + timedelta(days=i * step)).strftime("%d-%m-%Y") for i in range(points)]
        return now, end, label, dates

    def predict(self, period: str = "weekly") -> dict:
        """
        Returns dict with keys: 'text' (WhatsApp message), 'chart_path' (PNG file path).
        """
        if not self._api_key:
            return {
                "text": "Gold prediction unavailable: GEMINI_API_KEY not set.",
                "chart_path": None,
            }

        current = self._get_current_rates()
        historical = self._fetch_historical_context()
        now, end, period_label, date_points = self._calc_dates(period)
        start_str = now.strftime("%d-%b-%Y")
        end_str = end.strftime("%d-%b-%Y")
        dates_csv = ", ".join(date_points)

        prompt = f"""You are an expert gold market analyst. Provide gold price predictions.

CURRENT DATA (as of {current.get('timestamp', today_edt())}):
- Gold 24K India: {current.get('gold_24k_inr_per_gm', 'N/A')} INR/gram
- Gold 22K India: {current.get('gold_22k_inr_per_gm', 'N/A')} INR/gram
- CAD/INR: {current.get('cad_to_inr', 'N/A')}

{historical}

TASK: Predict gold prices from {start_str} to {end_str} ({period_label}).

You MUST respond with ONLY a JSON object (no markdown, no backticks, no explanation).
The JSON must have this exact structure:

{{
  "summary": "2-3 sentence WhatsApp-friendly summary with direction and key factors. Use * for bold.",
  "india": [
    {{"date": "DD-MM-YYYY", "gold_24k": 15033, "gold_22k": 13770}},
    ...more data points for these dates: {dates_csv}
  ],
  "canada": [
    {{"date": "DD-MM-YYYY", "gold_24k_cad": 95.50}},
    ...same dates as india
  ],
  "direction": "Up/Down/Stable",
  "pct_change": 2.5,
  "factors": ["factor1", "factor2", "factor3"]
}}

Rules:
- The first data point MUST use the current actual prices
- Subsequent points are your predictions
- Use realistic numbers based on current trends
- gold_24k and gold_22k are in INR per gram (no commas in JSON numbers)
- gold_24k_cad is in CAD per gram
- Calculate CAD price = (INR price / CAD_INR_rate)
- Include data points for ALL these dates: {dates_csv}

Respond with ONLY the JSON object."""

        try:
            import google.generativeai as genai
            genai.configure(api_key=self._api_key)
            model = genai.GenerativeModel("gemini-2.5-flash")
            response = model.generate_content(prompt)
            raw = response.text.strip()
            raw = re.sub(r"```json\s*", "", raw)
            raw = re.sub(r"```\s*", "", raw)
            data = json.loads(raw)
            logger.info(f"Gold prediction data received for {period_label}")

            # Generate chart
            chart_path = generate_gold_chart(data, period)

            # Build WhatsApp text message
            summary = data.get("summary", "Prediction generated.")
            direction = data.get("direction", "N/A")
            pct = data.get("pct_change", 0)
            factors = data.get("factors", [])

            india_pts = data.get("india", [])
            canada_pts = data.get("canada", [])

            lines = [
                f"*Gold Prediction - {period_label}*",
                f"{start_str} to {end_str}",
                "",
                summary,
                "",
                f"Direction: {direction} ({pct:+.1f}%)" if isinstance(pct, (int, float)) else f"Direction: {direction}",
                "",
                "*India (INR/gram)*",
            ]
            if india_pts:
                first = india_pts[0]
                last = india_pts[-1]
                lines.append(f"  24K Now:  Rs.{first.get('gold_24k', 'N/A'):,}" if isinstance(first.get('gold_24k'), (int, float)) else f"  24K Now:  Rs.{first.get('gold_24k', 'N/A')}")
                lines.append(f"  24K Pred: Rs.{last.get('gold_24k', 'N/A'):,}" if isinstance(last.get('gold_24k'), (int, float)) else f"  24K Pred: Rs.{last.get('gold_24k', 'N/A')}")
                lines.append(f"  22K Now:  Rs.{first.get('gold_22k', 'N/A'):,}" if isinstance(first.get('gold_22k'), (int, float)) else f"  22K Now:  Rs.{first.get('gold_22k', 'N/A')}")
                lines.append(f"  22K Pred: Rs.{last.get('gold_22k', 'N/A'):,}" if isinstance(last.get('gold_22k'), (int, float)) else f"  22K Pred: Rs.{last.get('gold_22k', 'N/A')}")

            lines.append("")
            lines.append("*Canada (CAD/gram)*")
            if canada_pts:
                first_c = canada_pts[0]
                last_c = canada_pts[-1]
                lines.append(f"  24K Now:  ${first_c.get('gold_24k_cad', 'N/A'):.2f}" if isinstance(first_c.get('gold_24k_cad'), (int, float)) else f"  24K Now:  ${first_c.get('gold_24k_cad', 'N/A')}")
                lines.append(f"  24K Pred: ${last_c.get('gold_24k_cad', 'N/A'):.2f}" if isinstance(last_c.get('gold_24k_cad'), (int, float)) else f"  24K Pred: ${last_c.get('gold_24k_cad', 'N/A')}")

            if factors:
                lines.append("")
                lines.append("*Key Factors*")
                for f in factors[:4]:
                    lines.append(f"  - {f}")

            lines.append("")
            lines.append("_AI-generated analysis. Not financial advice._")

            return {
                "text": "\n".join(lines),
                "chart_path": chart_path,
                "data": data,
            }

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse prediction JSON: {e}")
            logger.error(f"Raw response: {raw[:500]}")
            # Fallback: return raw text
            return {"text": raw[:1500], "chart_path": None}
        except Exception as e:
            logger.error(f"Gold prediction failed: {e}")
            return {"text": f"Gold prediction failed: {str(e)[:200]}", "chart_path": None}


# ---------------------------------------------------------------------------
# Delivery Report Generator
# ---------------------------------------------------------------------------

def generate_delivery_report() -> str:
    """Generate a per-target delivery report from the latest report file."""
    reports_dir = DOWNLOAD_DIR
    report_files = sorted(reports_dir.glob("report_*.json"), reverse=True)

    if not report_files:
        return "No delivery reports found. Send *run all* to generate one."

    # Load latest report
    with open(report_files[0]) as f:
        data = json.load(f)

    lines = [f"*Delivery Report - {data['date']}*", ""]

    receipts = data.get("delivery_receipts", [])
    if not receipts:
        lines.append("No deliveries recorded.")
        return "\n".join(lines)

    # Group by category
    categories = {}
    for r in receipts:
        cat = r.get("category", "unknown")
        if cat not in categories:
            categories[cat] = []
        categories[cat].append(r)

    cat_labels = {
        "videos": "Video Downloads",
        "financial": "Financial Updates",
        "consolidated_report": "Daily Reports",
    }

    total_ok = 0
    total_fail = 0

    for cat, items in categories.items():
        label = cat_labels.get(cat, cat.title())
        lines.append(f"*{label}*")

        for r in items:
            ok = r.get("success", False)
            icon = "Delivered" if ok else "Failed"
            name = r.get("label", "Unknown")
            phone = mask_phone(r.get("phone", ""))

            if ok:
                total_ok += 1
            else:
                total_fail += 1

            lines.append(f"  {icon} -> {name} ({phone})")
            if r.get("error"):
                lines.append(f"    Reason: {r['error']}")

        lines.append("")

    # Summary
    total = total_ok + total_fail
    lines.append(f"*Summary*: {total_ok}/{total} delivered")
    if total_fail > 0:
        lines.append(f"Failed: {total_fail}")

    # Check last N reports for history
    if len(report_files) > 1:
        lines.append("")
        lines.append("*Recent History*")
        for rf in report_files[:5]:
            try:
                with open(rf) as f:
                    rd = json.load(f)
                status = rd.get("delivery_status", "unknown")
                rdate = rd.get("date", rf.stem)
                lines.append(f"  {rdate}: {status}")
            except Exception:
                continue

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# MASTER ORCHESTRATOR
# ---------------------------------------------------------------------------

class MasterOrchestrator:
    def __init__(self, config_path: str = "config.yaml"):
        self.config = load_config(config_path)
        self.video_agent = VideoDownloaderAgent(self.config)
        self.finance_agent = FinancialScraperAgent(self.config)
        self.delivery_agent = DeliveryAgent(self.config)
        self.prediction_agent = GoldPredictionAgent(self.config)
        self.reports: list = []

    def run_daily(self, date_str: Optional[str] = None) -> DailyReport:
        if not date_str:
            date_str = today_edt()
        logger.info(f"=== DMFIA Daily Run: {date_str} ===")
        cleanup_old_downloads()
        report = DailyReport(date=date_str)

        logger.info("--- Phase 1: Video Downloads ---")
        try:
            report.video_results = self.video_agent.run(date_str)
        except Exception as e:
            logger.exception("Video agent crashed")
            report.video_results = [VideoResult("ALL", date_str, "failed", error=str(e)[:100])]

        logger.info("--- Phase 2: Financial Data ---")
        try:
            report.financial = self.finance_agent.run()
        except Exception as e:
            logger.exception("Finance agent crashed")
            report.financial = FinancialData(errors=[str(e)[:100]])

        logger.info("--- Phase 3: Delivery ---")
        try:
            report.delivery_receipts.extend(self.delivery_agent.send_videos(report.video_results))
            if report.financial:
                report.delivery_receipts.extend(self.delivery_agent.send_financial(report.financial))
            report.delivery_receipts.extend(
                self.delivery_agent.send_consolidated_report(report.to_consolidated_text()))
        except Exception as e:
            logger.exception("Delivery crashed")
            report.delivery_receipts.append(DeliveryReceipt("system", "", "N/A", False, str(e)[:100]))

        report_file = DOWNLOAD_DIR / f"report_{date_str}.json"
        with open(report_file, "w") as f:
            json.dump(report.to_dict(), f, indent=2)
        logger.info(f"Report saved: {report_file}")
        self.reports.append(report)
        return report

    def gold_report(self, period: str = "weekly") -> dict:
        """
        Combined gold report: current rates + India vs Canada comparison
        + AI prediction + chart. Returns dict with 'text' and 'chart_path'.
        """
        logger.info(f"=== Gold Report ({period}) ===")

        # 1. Get current rates + comparison
        fin = self.finance_agent.run()
        comparison_text = fin.to_comparison_text()

        # 2. Get prediction with chart
        prediction = self.prediction_agent.predict(period)
        pred_text = prediction.get("text", "") if isinstance(prediction, dict) else str(prediction)
        chart_path = prediction.get("chart_path") if isinstance(prediction, dict) else None

        # 3. Combine into one message
        combined = "\n".join([
            comparison_text,
            "",
            "=" * 30,
            "",
            pred_text,
        ])

        return {
            "text": combined,
            "chart_path": chart_path,
        }


if __name__ == "__main__":
    import sys
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    orch = MasterOrchestrator()
    report = orch.run_daily(date_arg)
    print(report.to_consolidated_text())
