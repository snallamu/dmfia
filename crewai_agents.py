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
    """Downloads Tamil serial episodes from tamildhool.tech.

    Flow:
      1. Fetch landing page (cloudscraper bypasses Cloudflare)
      2. Extract "Tap to watch" external link
      3. Fetch external page, find JW Player m3u8 URL
      4. Download with yt-dlp or ffmpeg
      5. Fallback: selenium-wire full browser flow
    """

    HEADERS = {"User-Agent": UA}

    def __init__(self, config: dict):
        self.config = config
        self.servers = config.get("servers", ["server1", "server2"])

    def _build_url(self, serial_cfg: dict, date_str: str) -> str:
        for key in ("landing_url", "player_url", "url_template"):
            if key in serial_cfg:
                return serial_cfg[key].replace("{date}", date_str)
        return serial_cfg.get("base_url", "").rstrip("/") + "/" + date_str + "/"

    # ----- Step 1: Fetch page (Cloudflare bypass) -----

    def _fetch_page(self, url: str) -> Optional[str]:
        """Fetch page HTML using cloudscraper to bypass Cloudflare 403."""
        logger.info(f"[fetch] {url}")
        # Try cloudscraper first (handles Cloudflare JS challenges)
        try:
            import cloudscraper
            scraper = cloudscraper.create_scraper(
                browser={"browser": "chrome", "platform": "windows", "mobile": False}
            )
            resp = scraper.get(url, timeout=30)
            if resp.status_code == 200:
                logger.info(f"[fetch] cloudscraper OK: {len(resp.text)} chars")
                return resp.text
            logger.warning(f"[fetch] cloudscraper status {resp.status_code}")
        except Exception as e:
            logger.warning(f"[fetch] cloudscraper failed: {e}")

        # Fallback: plain requests with browser headers
        try:
            headers = {
                "User-Agent": UA,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
            }
            resp = requests.get(url, headers=headers, timeout=20, allow_redirects=True)
            if resp.status_code == 200:
                logger.info(f"[fetch] requests OK: {len(resp.text)} chars")
                return resp.text
            logger.warning(f"[fetch] requests status {resp.status_code}")
        except Exception as e:
            logger.warning(f"[fetch] requests failed: {e}")

        # Fallback: curl subprocess
        try:
            result = subprocess.run(
                ["curl", "-sL", "-A", UA, "--max-time", "30", url],
                capture_output=True, text=True, timeout=35
            )
            if result.returncode == 0 and len(result.stdout) > 500:
                logger.info(f"[fetch] curl OK: {len(result.stdout)} chars")
                return result.stdout
        except Exception as e:
            logger.warning(f"[fetch] curl failed: {e}")

        return None

    # ----- Step 2: Extract "Tap to watch" link -----

    def _find_external_url(self, html: str, landing_url: str) -> Optional[str]:
        """Parse landing page for the external video link."""
        logger.info("[extract] Looking for 'Tap to watch' link")
        soup = BeautifulSoup(html, "lxml")

        # Pattern 1: <a> with "Tap to watch" text
        for a in soup.find_all("a", href=True):
            text = a.get_text(strip=True).lower()
            href = a["href"]
            if any(w in text for w in ["tap to watch", "watch now", "play video"]):
                logger.info(f"[extract] Found 'tap to watch': {href}")
                return href

        # Pattern 2: <a> with video_id parameter
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "video_id" in href and href.startswith("http"):
                logger.info(f"[extract] Found video_id link: {href}")
                return href

        # Pattern 3: External links (not same domain)
        from urllib.parse import urlparse
        landing_domain = urlparse(landing_url).netloc
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.startswith("http"):
                link_domain = urlparse(href).netloc
                if link_domain and link_domain != landing_domain:
                    text = a.get_text(strip=True).lower()
                    if any(w in text for w in ["watch", "tap", "play", "video", "stream"]):
                        logger.info(f"[extract] Found external link: {href}")
                        return href

        # Pattern 4: Regex in full HTML for video_id URLs
        matches = re.findall(r'https?://[^\s\'"<>]+\?video_id=[^\s\'"<>]+', html, re.I)
        if matches:
            logger.info(f"[extract] Found video_id in HTML: {matches[0]}")
            return matches[0]

        # Pattern 5: tamildhool.li links
        matches = re.findall(r'https?://[^\s\'"<>]*tamildhool\.li[^\s\'"<>]*', html, re.I)
        if matches:
            logger.info(f"[extract] Found tamildhool.li: {matches[0]}")
            return matches[0]

        logger.warning("[extract] No external video link found")
        return None

    # ----- Step 3: Extract m3u8 from video page -----

    def _find_m3u8_in_page(self, html: str) -> Optional[str]:
        """Extract m3u8 URL from page with JW Player or similar."""
        logger.info(f"[m3u8] Scanning {len(html)} chars for video URLs")

        # Pattern 1: Direct m3u8 URLs anywhere in source
        m3u8_urls = re.findall(r'https?://[^\s\'"<>\\]+\.m3u8[^\s\'"<>\\]*', html, re.I)
        if m3u8_urls:
            # Prefer non-master m3u8
            for url in m3u8_urls:
                if "master" not in url.lower():
                    logger.info(f"[m3u8] Found stream: {url}")
                    return url
            logger.info(f"[m3u8] Found master: {m3u8_urls[0]}")
            return m3u8_urls[0]

        # Pattern 2: JW Player config - file:"..." or source:"..."
        jw_patterns = [
            r'file\s*:\s*["\']([^"\']+\.m3u8[^"\']*)["\']',
            r'source\s*:\s*["\']([^"\']+\.m3u8[^"\']*)["\']',
            r'src\s*:\s*["\']([^"\']+\.m3u8[^"\']*)["\']',
            r'"file"\s*:\s*"([^"]+\.m3u8[^"]*)"',
            r'"source"\s*:\s*"([^"]+\.m3u8[^"]*)"',
            r'"src"\s*:\s*"([^"]+\.m3u8[^"]*)"',
            r'data-src\s*=\s*["\']([^"\']+\.m3u8[^"\']*)["\']',
        ]
        for pat in jw_patterns:
            match = re.search(pat, html, re.I)
            if match:
                url = match.group(1)
                logger.info(f"[m3u8] Found via JW pattern: {url}")
                return url

        # Pattern 3: MP4 URLs (fallback)
        mp4_urls = re.findall(r'https?://[^\s\'"<>\\]+\.mp4[^\s\'"<>\\]*', html, re.I)
        if mp4_urls:
            logger.info(f"[m3u8] Found MP4 fallback: {mp4_urls[0]}")
            return mp4_urls[0]

        # Pattern 4: Check iframes for player URLs
        soup = BeautifulSoup(html, "lxml")
        for iframe in soup.find_all("iframe"):
            src = iframe.get("src", "")
            if src and src.startswith("http"):
                logger.info(f"[m3u8] Found iframe player: {src}")
                # Fetch iframe page
                iframe_html = self._fetch_page(src)
                if iframe_html:
                    # Recursive search in iframe content
                    m3u8_in_iframe = re.findall(
                        r'https?://[^\s\'"<>\\]+\.m3u8[^\s\'"<>\\]*', iframe_html, re.I
                    )
                    if m3u8_in_iframe:
                        logger.info(f"[m3u8] Found in iframe: {m3u8_in_iframe[0]}")
                        return m3u8_in_iframe[0]
                    # Check for nested iframes
                    for pat in jw_patterns:
                        match = re.search(pat, iframe_html, re.I)
                        if match:
                            logger.info(f"[m3u8] Found in iframe JW: {match.group(1)}")
                            return match.group(1)

        logger.warning("[m3u8] No video URL found in page source")
        return None

    # ----- Step 4: Download methods -----

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
                logger.info(f"[yt-dlp] Done: {size_mb:.1f} MB")
                return True
            logger.warning(f"[yt-dlp] Failed (rc={result.returncode}): {result.stderr[:300]}")
            return False
        except subprocess.TimeoutExpired:
            logger.warning("[yt-dlp] Timeout 600s")
            return False
        except FileNotFoundError:
            logger.warning("[yt-dlp] not found")
            return False

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

    def _download_direct(self, url: str, output_path: str) -> bool:
        """Download a direct video URL (MP4/FLV) using wget or requests."""
        logger.info(f"[direct] Downloading: {url}")
        # Try wget first (handles redirects, resume)
        try:
            result = subprocess.run(
                ["wget", "-q", "-O", output_path, "--timeout=60",
                 "--user-agent", UA, url],
                capture_output=True, text=True, timeout=900
            )
            if result.returncode == 0 and Path(output_path).exists():
                sz = Path(output_path).stat().st_size / (1024 * 1024)
                if sz >= 1:
                    logger.info(f"[direct] wget done: {sz:.1f} MB")
                    return True
                Path(output_path).unlink(missing_ok=True)
        except Exception as e:
            logger.warning(f"[direct] wget failed: {e}")

        # Fallback: requests streaming download
        try:
            resp = requests.get(url, headers=self.HEADERS, timeout=120, stream=True)
            resp.raise_for_status()
            with open(output_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)
            sz = Path(output_path).stat().st_size / (1024 * 1024)
            if sz >= 1:
                logger.info(f"[direct] requests done: {sz:.1f} MB")
                return True
            Path(output_path).unlink(missing_ok=True)
        except Exception as e:
            logger.warning(f"[direct] requests failed: {e}")

        return False

    # ----- Step 5: Selenium fallback -----

    def _selenium_full_flow(self, landing_url: str) -> Optional[str]:
        """Phase 3: Full headless Chrome flow.
        1. Open landing page (tamildhool.tech)
        2. Click 'Tap to watch' -> navigates to NEW random external URL
        3. On external page: intercept HLS (.m3u8), MP4, .ts from network
        4. Return discovered video URL
        """
        logger.info(f"[selenium] Phase 3: Full browser flow")
        logger.info(f"[selenium] Landing: {landing_url}")
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

            def _scan_all_video():
                """Scan network requests for ANY video format: m3u8, mp4, ts."""
                m3u8_stream = None
                m3u8_master = None
                mp4_url = None
                ts_urls = []

                for req in driver.requests:
                    if not req.response or not req.url:
                        continue
                    url = req.url
                    url_lower = url.lower()
                    content_type = ""
                    if req.response.headers:
                        content_type = req.response.headers.get("Content-Type", "").lower()

                    # m3u8 streams
                    if ".m3u8" in url_lower:
                        if "master" in url_lower:
                            m3u8_master = url
                        else:
                            m3u8_stream = url

                    # MP4 files (check URL and content-type)
                    elif ".mp4" in url_lower and "image" not in url_lower:
                        if req.response.status_code == 200:
                            mp4_url = url
                    elif "video/mp4" in content_type or "video/x-flv" in content_type:
                        mp4_url = url

                    # HLS .ts segments
                    elif ".ts" in url_lower and "fonts" not in url_lower and "analytics" not in url_lower:
                        ts_urls.append(url)

                    # DASH mpd manifests
                    elif ".mpd" in url_lower:
                        logger.info(f"[selenium] Found DASH mpd: {url}")
                        return url

                    # application/vnd.apple.mpegurl
                    elif "mpegurl" in content_type:
                        m3u8_stream = url

                # Priority: stream m3u8 > master m3u8 > mp4 > reconstruct from .ts
                if m3u8_stream:
                    logger.info(f"[selenium] Intercepted m3u8 stream: {m3u8_stream}")
                    return m3u8_stream
                if m3u8_master:
                    logger.info(f"[selenium] Intercepted m3u8 master: {m3u8_master}")
                    return m3u8_master
                if mp4_url:
                    logger.info(f"[selenium] Intercepted MP4: {mp4_url}")
                    return mp4_url
                if ts_urls:
                    # Try to find m3u8 by guessing from .ts base URL
                    base = ts_urls[0].rsplit("/", 1)[0]
                    for suffix in ["/index.m3u8", "/playlist.m3u8", "/chunklist.m3u8", "/stream.m3u8"]:
                        candidate = base + suffix
                        try:
                            resp = requests.head(candidate, timeout=10, headers=HEADERS)
                            if resp.status_code == 200:
                                logger.info(f"[selenium] Reconstructed m3u8 from .ts: {candidate}")
                                return candidate
                        except Exception:
                            continue
                    logger.info(f"[selenium] Found {len(ts_urls)} .ts segments but no m3u8")

                return None

            play_sels = [
                ".jw-icon-display", ".jw-video", ".vjs-big-play-button",
                "video", ".play-btn", "#player", ".btn-play",
                "[class*='play']", "button[aria-label*='Play']",
                ".plyr__control--overlaid",  # Plyr player
            ]

            try:
                # ===== STEP 1: Open landing page =====
                logger.info(f"[selenium] Step 1: Opening landing page")
                driver.get(landing_url)
                time.sleep(6)

                # ===== STEP 2: Find "Tap to watch" and get NEW URL =====
                logger.info(f"[selenium] Step 2: Finding 'Tap to watch' link")
                external_url = None

                # Try clicking the link directly (this opens new URL in same tab)
                tap_xpaths = [
                    "//a[contains(text(),'Tap to watch')]",
                    "//a[contains(text(),'tap to watch')]",
                    "//a[contains(text(),'Watch')]",
                    "//a[contains(text(),'WATCH')]",
                    "//a[contains(@href,'video_id')]",
                    "//a[contains(@class,'watch')]",
                    "//a[contains(@class,'play')]",
                    "//a[contains(@class,'external')]",
                    "//a[contains(@class,'btn') and contains(@href,'http')]",
                ]

                # First: try to get href without clicking
                for xp in tap_xpaths:
                    try:
                        link = driver.find_element(By.XPATH, xp)
                        href = link.get_attribute("href")
                        if href and href.startswith("http"):
                            external_url = href
                            logger.info(f"[selenium] Found link href: {external_url}")
                            break
                    except Exception:
                        continue

                # If no explicit href, scan all <a> for external domains
                if not external_url:
                    from urllib.parse import urlparse
                    landing_domain = urlparse(landing_url).netloc
                    for a in driver.find_elements(By.TAG_NAME, "a"):
                        try:
                            href = a.get_attribute("href") or ""
                            if not href.startswith("http"):
                                continue
                            link_domain = urlparse(href).netloc
                            if link_domain and link_domain != landing_domain:
                                text = (a.text or "").lower()
                                # Prioritize video-related links
                                if "video_id" in href or any(w in text for w in ["watch", "tap", "play", "video"]):
                                    external_url = href
                                    logger.info(f"[selenium] Found external link: {external_url}")
                                    break
                        except Exception:
                            continue

                # If still nothing, try clicking and see where it goes
                if not external_url:
                    for xp in tap_xpaths[:3]:
                        try:
                            link = driver.find_element(By.XPATH, xp)
                            try:
                                del driver.requests
                            except Exception:
                                pass
                            link.click()
                            time.sleep(5)
                            # Check if URL changed (navigated to new page)
                            new_url = driver.current_url
                            if new_url != landing_url:
                                external_url = new_url
                                logger.info(f"[selenium] Clicked through to: {external_url}")
                            break
                        except Exception:
                            continue

                if not external_url:
                    logger.warning("[selenium] No 'Tap to watch' link found")
                    return None

                # ===== STEP 3: Navigate to NEW external URL =====
                logger.info(f"[selenium] Step 3: Opening NEW URL: {external_url}")
                try:
                    del driver.requests  # Clear all previous network captures
                except Exception:
                    pass

                # Only navigate if we haven't already clicked through
                if driver.current_url != external_url:
                    driver.get(external_url)
                time.sleep(8)

                logger.info(f"[selenium] Current page: {driver.current_url}")
                logger.info(f"[selenium] Page title: {driver.title}")

                # ===== STEP 4: Scan for auto-loaded video =====
                video_url = _scan_all_video()
                if video_url:
                    return video_url

                # ===== STEP 5: Click play buttons on external page =====
                logger.info(f"[selenium] Step 5: Clicking play buttons")
                for sel in play_sels:
                    try:
                        el = driver.find_element(By.CSS_SELECTOR, sel)
                        logger.info(f"[selenium] Clicking: {sel}")
                        el.click()
                        time.sleep(4)
                        video_url = _scan_all_video()
                        if video_url:
                            return video_url
                    except Exception:
                        continue

                # ===== STEP 6: Click player tabs (JW Player, Thirai One) =====
                logger.info(f"[selenium] Step 6: Clicking player tabs")
                tab_xpaths = [
                    "//a[contains(text(),'JW Player')]",
                    "//a[contains(text(),'Thirai')]",
                    "//button[contains(text(),'JW')]",
                    "//*[contains(@class,'tab')]",
                ]
                for xp in tab_xpaths:
                    try:
                        tab = driver.find_element(By.XPATH, xp)
                        logger.info(f"[selenium] Clicking tab: {tab.text}")
                        try:
                            del driver.requests
                        except Exception:
                            pass
                        tab.click()
                        time.sleep(5)
                        for sel in play_sels:
                            try:
                                driver.find_element(By.CSS_SELECTOR, sel).click()
                                time.sleep(3)
                            except Exception:
                                continue
                        video_url = _scan_all_video()
                        if video_url:
                            return video_url
                    except Exception:
                        continue

                # ===== STEP 7: Check inside iframes =====
                iframes = driver.find_elements(By.TAG_NAME, "iframe")
                logger.info(f"[selenium] Step 7: Checking {len(iframes)} iframes")
                for i, iframe in enumerate(iframes):
                    try:
                        src = iframe.get_attribute("src") or ""
                        logger.info(f"[selenium] iframe[{i}]: {src[:80]}")
                        driver.switch_to.frame(iframe)
                        time.sleep(4)

                        for sel in play_sels:
                            try:
                                driver.find_element(By.CSS_SELECTOR, sel).click()
                                time.sleep(3)
                            except Exception:
                                continue

                        # Check nested iframes (video players often double-iframe)
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

                        video_url = _scan_all_video()
                        if video_url:
                            return video_url
                        driver.switch_to.default_content()
                    except Exception:
                        driver.switch_to.default_content()

                # ===== STEP 8: Final wait and scan =====
                logger.info(f"[selenium] Step 8: Final wait (15s)")
                time.sleep(15)
                video_url = _scan_all_video()
                if video_url:
                    return video_url

                # ===== STEP 9: Log all captured URLs for debugging =====
                logger.warning("[selenium] No video found. Captured URLs:")
                for req in driver.requests:
                    if req.response:
                        ct = req.response.headers.get("Content-Type", "") if req.response.headers else ""
                        if any(x in ct.lower() for x in ["video", "mpegurl", "octet"]) or \
                           any(x in req.url.lower() for x in [".m3u8", ".mp4", ".ts", ".mpd", ".flv"]):
                            logger.warning(f"  {req.response.status_code} {ct[:30]} {req.url[:120]}")

                # Return external URL so yt-dlp can try
                return f"EXTERNAL:{external_url}"

            finally:
                driver.quit()
        except Exception as e:
            logger.error(f"[selenium] Phase 3 failed: {e}")
            return None

    # ----- Main download pipeline -----

    def download_serial(self, serial_cfg: dict, date_str: str) -> VideoResult:
        name = serial_cfg["name"]
        filename = f"{name}_{date_str}.mp4".replace(" ", "_")
        output_path = str(DOWNLOAD_DIR / filename)
        landing_url = self._build_url(serial_cfg, date_str)

        logger.info(f"{'='*50}")
        logger.info(f"DOWNLOAD: {name} | {date_str}")
        logger.info(f"Landing:  {landing_url}")
        logger.info(f"{'='*50}")

        try:
            # ===========================================================
            # PHASE 1: Lightweight approach (cloudscraper + parsing)
            # ===========================================================

            # Step 1: Fetch landing page
            landing_html = self._fetch_page(landing_url)
            external_url = None

            if landing_html:
                # Step 2: Find "Tap to watch" link
                external_url = self._find_external_url(landing_html, landing_url)

            if external_url:
                logger.info(f"[phase1] External URL: {external_url}")

                # Step 3: Try yt-dlp directly on external URL
                if self._download_with_ytdlp(external_url, output_path):
                    return VideoResult(name, date_str, "success", file_path=output_path)

                # Step 4: Fetch external page and find m3u8
                ext_html = self._fetch_page(external_url)
                if ext_html:
                    m3u8_url = self._find_m3u8_in_page(ext_html)
                    if m3u8_url:
                        logger.info(f"[phase1] m3u8 found: {m3u8_url}")
                        if self._download_with_ffmpeg(m3u8_url, output_path, referer=external_url):
                            return VideoResult(name, date_str, "success", file_path=output_path)
                        if self._download_with_ytdlp(m3u8_url, output_path):
                            return VideoResult(name, date_str, "success", file_path=output_path)

            # ===========================================================
            # PHASE 2: Try yt-dlp directly on landing page
            # ===========================================================
            logger.info("[phase2] Trying yt-dlp on landing page")
            if self._download_with_ytdlp(landing_url, output_path):
                return VideoResult(name, date_str, "success", file_path=output_path)

            # ===========================================================
            # PHASE 3: Selenium full browser flow (last resort)
            # Opens landing -> clicks "Tap to watch" -> NEW URL
            # -> intercepts HLS/MP4/M3U8 from network
            # ===========================================================
            logger.info("[phase3] Selenium full browser flow")
            result_url = self._selenium_full_flow(landing_url)

            if result_url:
                if result_url.startswith("EXTERNAL:"):
                    ext = result_url[9:]
                    logger.info(f"[phase3] Trying yt-dlp on external: {ext}")
                    if self._download_with_ytdlp(ext, output_path):
                        return VideoResult(name, date_str, "success", file_path=output_path)
                elif ".m3u8" in result_url or ".mpd" in result_url:
                    logger.info(f"[phase3] HLS/DASH found: {result_url}")
                    if self._download_with_ffmpeg(result_url, output_path, referer=landing_url):
                        return VideoResult(name, date_str, "success", file_path=output_path)
                    if self._download_with_ytdlp(result_url, output_path):
                        return VideoResult(name, date_str, "success", file_path=output_path)
                elif ".mp4" in result_url:
                    logger.info(f"[phase3] MP4 found: {result_url}")
                    # Direct MP4: download with yt-dlp (handles redirects/headers)
                    if self._download_with_ytdlp(result_url, output_path):
                        return VideoResult(name, date_str, "success", file_path=output_path)
                    # Or wget/curl fallback
                    if self._download_direct(result_url, output_path):
                        return VideoResult(name, date_str, "success", file_path=output_path)
                else:
                    # Unknown format, let yt-dlp figure it out
                    if self._download_with_ytdlp(result_url, output_path):
                        return VideoResult(name, date_str, "success", file_path=output_path)

            return VideoResult(name, date_str, "failed",
                               error="All phases failed. Check logs for details.")

        except Exception as e:
            logger.exception(f"Download crashed for {name}")
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
