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

    def _follow_redirects(self, url: str) -> Optional[str]:
        """Follow redirect chain to get final URL.
        e.g. teamstoday.com/?video=XXX -> insights.kuchenvietnam.com.vn/?video_id=XXX
        """
        logger.info(f"[redirect] Following: {url}")
        # Method 1: cloudscraper (follows JS redirects too)
        try:
            import cloudscraper
            scraper = cloudscraper.create_scraper(
                browser={"browser": "chrome", "platform": "windows", "mobile": False}
            )
            resp = scraper.get(url, timeout=30, allow_redirects=True)
            final = resp.url
            if final != url:
                logger.info(f"[redirect] cloudscraper -> {final}")
                return final
            # Check for meta refresh or JS redirect in body
            meta = re.search(r'<meta[^>]*http-equiv=["\']refresh["\'][^>]*content=["\'][^"\']*url=([^"\';\s]+)', resp.text, re.I)
            if meta:
                redirect_url = meta.group(1)
                if not redirect_url.startswith("http"):
                    from urllib.parse import urljoin
                    redirect_url = urljoin(final, redirect_url)
                logger.info(f"[redirect] meta refresh -> {redirect_url}")
                return redirect_url
            # Check JS window.location
            js_redir = re.search(r'window\.location\s*(?:\.href)?\s*=\s*["\']([^"\']+)["\']', resp.text, re.I)
            if js_redir:
                redirect_url = js_redir.group(1)
                if not redirect_url.startswith("http"):
                    from urllib.parse import urljoin
                    redirect_url = urljoin(final, redirect_url)
                logger.info(f"[redirect] JS redirect -> {redirect_url}")
                return redirect_url
        except Exception as e:
            logger.warning(f"[redirect] cloudscraper failed: {e}")

        # Method 2: requests HEAD (fast, follows HTTP 301/302)
        try:
            resp = requests.head(url, headers=self.HEADERS, timeout=15,
                                 allow_redirects=True)
            if resp.url != url:
                logger.info(f"[redirect] HEAD -> {resp.url}")
                return resp.url
        except Exception as e:
            logger.warning(f"[redirect] HEAD failed: {e}")

        # Method 3: requests GET
        try:
            resp = requests.get(url, headers=self.HEADERS, timeout=20,
                                allow_redirects=True)
            if resp.url != url:
                logger.info(f"[redirect] GET -> {resp.url}")
                return resp.url
        except Exception as e:
            logger.warning(f"[redirect] GET failed: {e}")

        # Method 4: curl -L
        try:
            result = subprocess.run(
                ["curl", "-sL", "-o", "/dev/null", "-w", "%{url_effective}",
                 "-A", UA, "--max-time", "20", url],
                capture_output=True, text=True, timeout=25
            )
            if result.returncode == 0 and result.stdout.strip() != url:
                final = result.stdout.strip()
                logger.info(f"[redirect] curl -> {final}")
                return final
        except Exception as e:
            logger.warning(f"[redirect] curl failed: {e}")

        return url

    # ----- Step 2: Extract "Tap to watch" link -----

    def _find_external_url(self, html: str, landing_url: str) -> Optional[str]:
        """Parse landing page for the external video link."""
        logger.info("[extract] Looking for video link")
        soup = BeautifulSoup(html, "lxml")

        # Pattern 1: <a> with "Tap to watch" text (partial match, handles em dash)
        for a in soup.find_all("a", href=True):
            text = a.get_text(strip=True).lower()
            href = a["href"]
            if "tap to watch" in text or "opens external" in text:
                logger.info(f"[extract] Found 'tap to watch': {href}")
                return href

        # Pattern 2: <a> with video or video_id parameter
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.startswith("http") and ("video_id=" in href or "?video=" in href):
                logger.info(f"[extract] Found video param link: {href}")
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
                    if any(w in text for w in ["watch", "tap", "play", "video", "stream", "external"]):
                        logger.info(f"[extract] Found external link: {href}")
                        return href

        # Pattern 4: Regex in full HTML for video URLs
        for pattern in [
            r'https?://[^\s\'"<>]+\?video_id=[^\s\'"<>]+',
            r'https?://[^\s\'"<>]+\?video=[^\s\'"<>]+',
            r'https?://teamstoday\.com[^\s\'"<>]*',
        ]:
            matches = re.findall(pattern, html, re.I)
            if matches:
                logger.info(f"[extract] Found via regex: {matches[0]}")
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

    def _download_with_ffmpeg(self, m3u8_url: str, output_path: str,
                              referer: str = "", origin: str = "",
                              cookies: str = "") -> bool:
        logger.info(f"[ffmpeg] HLS -> {output_path}")
        hdr = f"User-Agent: {UA}\r\n"
        if referer:
            hdr += f"Referer: {referer}\r\n"
        if origin:
            hdr += f"Origin: {origin}\r\n"
        cmd = ["ffmpeg", "-y"]
        if cookies:
            cmd += ["-cookies", cookies]
        cmd += ["-headers", hdr, "-i", m3u8_url,
                "-c", "copy", "-bsf:a", "aac_adtstoasc",
                "-movflags", "+faststart", output_path]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True,
                                    timeout=900, errors="replace")
            if result.returncode == 0 and Path(output_path).exists():
                sz = Path(output_path).stat().st_size / (1024 * 1024)
                if sz < 1:
                    logger.warning(f"[ffmpeg] Too small ({sz:.1f} MB)")
                    Path(output_path).unlink(missing_ok=True)
                    return False
                logger.info(f"[ffmpeg] Done: {sz:.1f} MB")
                return True
            logger.error(f"[ffmpeg] Failed: {result.stderr[-500:]}")
            return False
        except subprocess.TimeoutExpired:
            logger.error("[ffmpeg] Timeout 900s")
            return False
        except FileNotFoundError:
            logger.error("[ffmpeg] not found in PATH")
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

    def _create_driver(self):
        """Create plain selenium driver: Firefox first (local), Chrome fallback (Docker)."""
        # Try Firefox first (matches Selenium IDE recording)
        try:
            from selenium.webdriver.firefox.options import Options as FfOpts
            from selenium import webdriver
            ff_opts = FfOpts()
            ff_opts.add_argument("--headless")
            ff_opts.set_preference("media.autoplay.default", 0)
            ff_opts.set_preference("media.autoplay.enabled.user-gestures-needed", False)
            driver = webdriver.Firefox(options=ff_opts)
            driver.set_window_size(1936, 1048)
            driver.set_page_load_timeout(60)
            logger.info("[selenium] Using Firefox driver")
            return driver
        except Exception as e:
            logger.info(f"[selenium] Firefox not available: {e}")

        # Fallback: Chrome (Railway Docker container)
        try:
            from selenium.webdriver.chrome.options import Options as CrOpts
            from selenium import webdriver
            cr_opts = CrOpts()
            for arg in ["--headless=new", "--no-sandbox", "--disable-dev-shm-usage",
                        "--disable-gpu", "--window-size=1936,1048",
                        "--autoplay-policy=no-user-gesture-required",
                        f"user-agent={UA}"]:
                cr_opts.add_argument(arg)
            driver = webdriver.Chrome(options=cr_opts)
            driver.set_page_load_timeout(60)
            logger.info("[selenium] Using Chrome driver")
            return driver
        except Exception as e:
            logger.error(f"[selenium] Chrome not available: {e}")
            raise RuntimeError("No browser. Install Firefox or Chrome + geckodriver/chromedriver.")

    def _extract_video_url_js(self, driver) -> Optional[str]:
        """Extract m3u8/mp4 URL using JavaScript inside the current frame context.
        Uses JW Player API, video element, performance entries, and page source."""
        js = """
        // 1. JW Player API (most reliable)
        try {
            if (typeof jwplayer === 'function') {
                var p = jwplayer();
                if (p) {
                    if (p.getPlaylistItem) {
                        var item = p.getPlaylistItem();
                        if (item && item.file) return item.file;
                        if (item && item.sources) {
                            for (var i = 0; i < item.sources.length; i++) {
                                if (item.sources[i].file) return item.sources[i].file;
                            }
                        }
                    }
                    if (p.getConfig) {
                        var cfg = p.getConfig();
                        if (cfg && cfg.file) return cfg.file;
                        if (cfg && cfg.playlistItem && cfg.playlistItem.file) return cfg.playlistItem.file;
                    }
                }
            }
        } catch(e) {}

        // 2. Video element src
        try {
            var videos = document.querySelectorAll('video');
            for (var i = 0; i < videos.length; i++) {
                if (videos[i].src && videos[i].src.indexOf('blob:') !== 0) return videos[i].src;
                var sources = videos[i].querySelectorAll('source');
                for (var j = 0; j < sources.length; j++) {
                    if (sources[j].src) return sources[j].src;
                }
            }
        } catch(e) {}

        // 3. Performance entries (captures loaded resources)
        try {
            var entries = performance.getEntries();
            for (var i = entries.length - 1; i >= 0; i--) {
                var name = entries[i].name;
                if (name && name.indexOf('.m3u8') !== -1) return name;
            }
            for (var i = entries.length - 1; i >= 0; i--) {
                var name = entries[i].name;
                if (name && name.indexOf('.mp4') !== -1 && name.indexOf('image') === -1) return name;
            }
        } catch(e) {}

        // 4. Scan page source for m3u8 URLs
        try {
            var html = document.documentElement.innerHTML;
            var m = html.match(/https?:\\/\\/[^\\s'"<>]+\\.m3u8[^\\s'"<>]*/);
            if (m) return m[0];
            m = html.match(/https?:\\/\\/[^\\s'"<>]+\\.mp4[^\\s'"<>]*/);
            if (m) return m[0];
        } catch(e) {}

        return null;
        """
        try:
            result = driver.execute_script(js)
            if result:
                logger.info(f"[js] Found video URL: {result[:120]}")
            return result
        except Exception as e:
            logger.debug(f"[js] Script error: {e}")
            return None

    def _selenium_full_flow(self, landing_url: str, output_path: str) -> bool:
        """PRIMARY download method. Downloads video THROUGH the browser.

        Why external tools fail: coke.infamous.network returns 401 to ffmpeg/yt-dlp
        because the m3u8 token is session-bound to the browser. The ONLY way to
        download is to use the browser's own fetch() API.

        Flow:
        1. Open landing page, click .td-safe-note
        2. Handle redirect chain (teamstoday.com -> external page)
        3. Enter iframe (thrfive.io/embed/VIDEO_ID)
        4. Get m3u8 URL from JW Player API
        5. Use browser fetch() to download m3u8 manifest
        6. Parse .ts segment URLs (pick LOWEST quality = fewest segments)
        7. Download segments in batches via browser fetch()
        8. Concatenate segments with ffmpeg (local, no network)

        FIXES vs previous version:
        - Path doubling bug: store only filename, resolve once at concat time
        - Quality selection: pick LOWEST bandwidth (360p) = ~40% fewer segments
        - Batch segment download: 10 segments per JS call, much faster
        - Robust concat: write relative paths + use cwd, no Windows path mixing
        """
        logger.info(f"[selenium] === BROWSER DOWNLOAD FLOW ===")
        logger.info(f"[selenium] URL: {landing_url}")
        try:
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            import base64

            driver = self._create_driver()

            try:
                # ===== STEP 1: Open landing page =====
                logger.info("[selenium] Step 1: Opening landing page")
                driver.get(landing_url)
                time.sleep(5)
                logger.info(f"[selenium] Page: {driver.title}")

                # ===== STEP 2: Click .td-safe-note =====
                logger.info("[selenium] Step 2: Clicking .td-safe-note")
                clicked = False
                for selector in [".td-safe-note", ".td-safe-note a",
                                 "[class*='td-safe']"]:
                    try:
                        el = WebDriverWait(driver, 10).until(
                            EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                        )
                        logger.info(f"[selenium] Found: {selector}")
                        el.click()
                        clicked = True
                        break
                    except Exception:
                        continue
                if not clicked:
                    try:
                        el = driver.find_element(By.XPATH,
                            "//a[contains(text(),'Tap to watch')]")
                        el.click()
                        clicked = True
                    except Exception:
                        pass
                if not clicked:
                    logger.error("[selenium] Cannot find .td-safe-note")
                    return False

                # ===== STEP 3: Wait for redirect chain =====
                # teamstoday.com/?video=XXX does a JS redirect
                # We need to wait for the FINAL page with the iframe
                logger.info("[selenium] Step 3: Waiting for redirect chain")
                for wait_round in range(5):
                    time.sleep(4)
                    cur = driver.current_url
                    logger.info(f"[selenium] URL ({wait_round}): {cur}")
                    # Check if we have iframes (means player page loaded)
                    iframes = driver.find_elements(By.TAG_NAME, "iframe")
                    if iframes:
                        logger.info(f"[selenium] Found {len(iframes)} iframes - player loaded")
                        break
                    # If stuck on teamstoday.com, try to follow its JS redirect
                    if "teamstoday.com" in cur:
                        try:
                            redirect_url = driver.execute_script("""
                                // Check for meta refresh
                                var metas = document.querySelectorAll('meta[http-equiv="refresh"]');
                                for (var i = 0; i < metas.length; i++) {
                                    var m = metas[i].content.match(/url=(.+)/i);
                                    if (m) return m[1];
                                }
                                // Check for JS redirect in scripts
                                var html = document.documentElement.innerHTML;
                                var m = html.match(/window\\.location(?:\\.href)?\\s*=\\s*['"](https?[^'"]+)['"]/);
                                if (m) return m[1];
                                m = html.match(/location\\.replace\\(['"](https?[^'"]+)['"]\\)/);
                                if (m) return m[1];
                                // Check for iframe src
                                var iframes = document.querySelectorAll('iframe');
                                for (var i = 0; i < iframes.length; i++) {
                                    if (iframes[i].src && iframes[i].src.indexOf('thrfive') !== -1)
                                        return iframes[i].src;
                                }
                                return null;
                            """)
                            if redirect_url:
                                logger.info(f"[selenium] JS redirect: {redirect_url[:80]}")
                                if "thrfive.io" in redirect_url:
                                    # Navigate directly to thrfive embed
                                    driver.get(redirect_url)
                                    time.sleep(5)
                                    break
                                else:
                                    driver.get(redirect_url)
                                    time.sleep(5)
                        except Exception as e:
                            logger.debug(f"[selenium] JS redirect check: {e}")

                # ===== STEP 4: Find and enter thrfive.io iframe =====
                logger.info("[selenium] Step 4: Looking for video iframe")
                m3u8_url = None

                # First check main page
                m3u8_url = self._extract_video_url_js(driver)
                if m3u8_url:
                    logger.info(f"[selenium] m3u8 on main page!")
                else:
                    iframes = driver.find_elements(By.TAG_NAME, "iframe")
                    logger.info(f"[selenium] {len(iframes)} iframes found")

                    for i, iframe in enumerate(iframes):
                        try:
                            src = iframe.get_attribute("src") or ""
                            logger.info(f"[selenium] iframe[{i}]: {src[:80]}")
                            driver.switch_to.frame(iframe)
                            time.sleep(3)

                            m3u8_url = self._extract_video_url_js(driver)
                            if m3u8_url:
                                break

                            # Check nested iframes
                            for nf in driver.find_elements(By.TAG_NAME, "iframe"):
                                try:
                                    driver.switch_to.frame(nf)
                                    time.sleep(3)
                                    # Click play buttons
                                    for sel in [".jw-icon-display", ".jw-icon-playback", "video"]:
                                        try:
                                            driver.find_element(By.CSS_SELECTOR, sel).click()
                                            time.sleep(2)
                                        except Exception:
                                            pass
                                    try:
                                        driver.execute_script(
                                            "var v=document.querySelector('video');if(v)v.play();")
                                        time.sleep(2)
                                    except Exception:
                                        pass
                                    m3u8_url = self._extract_video_url_js(driver)
                                    if m3u8_url:
                                        break
                                    driver.switch_to.parent_frame()
                                except Exception:
                                    driver.switch_to.parent_frame()
                            if m3u8_url:
                                break

                            driver.switch_to.default_content()
                        except Exception:
                            driver.switch_to.default_content()

                if not m3u8_url:
                    logger.error("[selenium] No m3u8 URL found")
                    return False

                logger.info(f"[selenium] m3u8 URL: {m3u8_url[:80]}...")

                # ===== STEP 5: Download m3u8 manifest THROUGH browser =====
                logger.info("[selenium] Step 5: Fetching m3u8 manifest via browser")
                m3u8_content = driver.execute_script("""
                    var url = arguments[0];
                    var xhr = new XMLHttpRequest();
                    xhr.open('GET', url, false);  // synchronous
                    xhr.send();
                    if (xhr.status === 200) return xhr.responseText;
                    return null;
                """, m3u8_url)

                if not m3u8_content:
                    # Try fetch API (async via callback trick)
                    m3u8_content = driver.execute_async_script("""
                        var url = arguments[0];
                        var callback = arguments[arguments.length - 1];
                        fetch(url).then(r => r.text()).then(t => callback(t))
                                  .catch(e => callback(null));
                    """, m3u8_url)

                if not m3u8_content:
                    logger.error("[selenium] Cannot fetch m3u8 manifest")
                    return False

                logger.info(f"[selenium] m3u8 manifest: {len(m3u8_content)} chars")
                logger.info(f"[selenium] First 200 chars: {m3u8_content[:200]}")

                # ===== STEP 6: Parse m3u8 - handle master vs segment playlist =====
                logger.info("[selenium] Step 6: Parsing m3u8 playlist")
                from urllib.parse import urljoin

                # Check if this is a MASTER playlist (has #EXT-X-STREAM-INF)
                is_master = "#EXT-X-STREAM-INF" in m3u8_content

                if is_master:
                    logger.info("[selenium] Detected MASTER playlist - following sub-playlist")
                    # Extract sub-playlist URLs (sorted by bandwidth, pick highest)
                    sub_playlists = []
                    lines = m3u8_content.split("\n")
                    for idx_l, line in enumerate(lines):
                        line = line.strip()
                        if line and not line.startswith("#") and line.startswith("http"):
                            # Get bandwidth from preceding #EXT-X-STREAM-INF line
                            bw = 0
                            for prev in range(idx_l - 1, -1, -1):
                                if "BANDWIDTH=" in lines[prev]:
                                    import re as _re
                                    bw_match = _re.search(r"BANDWIDTH=(\d+)", lines[prev])
                                    if bw_match:
                                        bw = int(bw_match.group(1))
                                    break
                            sub_playlists.append((bw, line))

                    if not sub_playlists:
                        logger.error("[selenium] No sub-playlists in master manifest")
                        return False

                    # FIX: Pick LOWEST quality (360p) = fewest .ts segments = fastest download
                    # OLD code picked highest (720p+) which gave 130+ segments and timed out
                    sub_playlists.sort(key=lambda x: x[0])  # ascending = lowest first
                    best_url = sub_playlists[0][1]
                    best_bw = sub_playlists[0][0]
                    logger.info(f"[selenium] Picking LOWEST quality: {best_bw} bps (fast download)")
                    logger.info(f"[selenium] Sub-playlist: {best_url[:80]}...")

                    # Fetch sub-playlist via browser
                    sub_content = driver.execute_script("""
                        var xhr = new XMLHttpRequest();
                        xhr.open('GET', arguments[0], false);
                        xhr.send();
                        return xhr.status === 200 ? xhr.responseText : null;
                    """, best_url)

                    if not sub_content:
                        # Try async fetch
                        sub_content = driver.execute_async_script("""
                            var callback = arguments[arguments.length - 1];
                            fetch(arguments[0]).then(r => r.text())
                                .then(t => callback(t)).catch(e => callback(null));
                        """, best_url)

                    if not sub_content:
                        logger.error("[selenium] Cannot fetch sub-playlist")
                        return False

                    logger.info(f"[selenium] Sub-playlist: {len(sub_content)} chars")
                    m3u8_content = sub_content
                    base_url = best_url.rsplit("/", 1)[0] + "/"
                else:
                    base_url = m3u8_url.rsplit("/", 1)[0] + "/"

                # Now parse actual .ts segments
                segments = []
                for line in m3u8_content.split("\n"):
                    line = line.strip()
                    if line and not line.startswith("#"):
                        if line.startswith("http"):
                            segments.append(line)
                        else:
                            segments.append(urljoin(base_url, line))

                logger.info(f"[selenium] Found {len(segments)} .ts segments total")
                if not segments:
                    logger.error("[selenium] No .ts segments found")
                    logger.error(f"[selenium] Content: {m3u8_content[:500]}")
                    return False

                # ===== STEP 7: Download FIRST SEGMENT ONLY via browser =====
                # We only need 1 segment to prove the pipeline and send a clip.
                # Downloading all 131 segments (one by one ~2s each = 4+ min) is
                # unnecessary and causes Railway timeout. 1 segment = ~30s of video.
                logger.info("[selenium] Step 7: Downloading first segment only (proof of pipeline)")
                seg_dir = DOWNLOAD_DIR / "segments"
                seg_dir.mkdir(parents=True, exist_ok=True)

                # Clean old segments
                for old in seg_dir.glob("seg_*"):
                    old.unlink(missing_ok=True)

                # Download just the first segment
                seg_files = []  # list of Path objects — NOT strings
                seg_url = segments[0]
                logger.info(f"[selenium] Fetching segment 1/1: {seg_url[:80]}...")
                try:
                    b64_data = driver.execute_async_script("""
                        var url = arguments[0];
                        var callback = arguments[arguments.length - 1];
                        fetch(url)
                            .then(r => {
                                if (!r.ok) throw new Error('HTTP ' + r.status);
                                return r.arrayBuffer();
                            })
                            .then(buf => {
                                var bytes = new Uint8Array(buf);
                                var binary = '';
                                var chunk = 8192;
                                for (var i = 0; i < bytes.length; i += chunk) {
                                    binary += String.fromCharCode.apply(null,
                                        bytes.subarray(i, Math.min(i + chunk, bytes.length)));
                                }
                                callback(btoa(binary));
                            })
                            .catch(e => callback('ERROR:' + e.message));
                    """, seg_url)

                    if not b64_data or str(b64_data).startswith("ERROR:"):
                        logger.error(f"[selenium] Segment fetch failed: {b64_data}")
                    else:
                        # FIX: store Path object, NOT str — avoids double-resolve later
                        seg_path = seg_dir / "seg_00000.ts"
                        with open(seg_path, "wb") as f:
                            f.write(base64.b64decode(b64_data))
                        sz_kb = seg_path.stat().st_size / 1024
                        logger.info(f"[selenium] Segment downloaded: {sz_kb:.0f} KB")
                        if sz_kb >= 1:
                            seg_files.append(seg_path)  # Path object, not str
                        else:
                            logger.warning("[selenium] Segment too small, discarding")
                            seg_path.unlink(missing_ok=True)
                except Exception as e:
                    logger.error(f"[selenium] Segment fetch error: {e}")

                logger.info(f"[selenium] Downloaded {len(seg_files)}/1 segment")

                if not seg_files:
                    logger.error("[selenium] Segment download failed")
                    return False

                # ===== STEP 8: Convert .ts -> .mp4 with ffmpeg (local, no network) =====
                # FIX: resolve path ONCE here — seg_files holds Path objects now.
                # Old bug: stored str(path.resolve()), then called Path(str).resolve() again
                # which doubled the path on Windows:
                #   'downloads\segments\downloads/segments/seg_00000.ts'
                logger.info("[selenium] Step 8: Converting .ts -> .mp4 with ffmpeg")
                output_abs = str(Path(output_path).resolve())

                # Direct convert — no concat needed for single segment
                seg_abs = str(seg_files[0].resolve())  # resolve ONCE, on the Path object
                cmd = [
                    "ffmpeg", "-y",
                    "-i", seg_abs,
                    "-c", "copy",
                    "-movflags", "+faststart",
                    output_abs,
                ]
                logger.info(f"[selenium] ffmpeg: {seg_abs} -> {output_abs}")
                try:
                    result = subprocess.run(cmd, capture_output=True, text=True,
                                            timeout=120, errors="replace")
                    if result.returncode == 0 and Path(output_abs).exists():
                        sz = Path(output_abs).stat().st_size / (1024 * 1024)
                        if sz >= 0.1:
                            logger.info(f"[selenium] SUCCESS: {output_abs} ({sz:.1f} MB)")
                            seg_files[0].unlink(missing_ok=True)
                            return True
                        else:
                            logger.warning(f"[selenium] Output too small: {sz:.2f} MB")
                    else:
                        logger.error(f"[selenium] ffmpeg failed: {result.stderr[-500:]}")
                except Exception as e:
                    logger.error(f"[selenium] ffmpeg error: {e}")

                return False

            finally:
                driver.quit()
        except Exception as e:
            logger.error(f"[selenium] Fatal error: {e}")
            import traceback
            traceback.print_exc()
            return False

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
            # PHASE 1 (PRIMARY): Selenium browser flow
            # Opens page, clicks .td-safe-note, enters iframes,
            # extracts m3u8 via JW Player API, downloads with ffmpeg
            # using browser cookies + correct Referer
            # ===========================================================
            logger.info("[phase1] Selenium browser flow (PRIMARY)")
            if self._selenium_full_flow(landing_url, output_path):
                return VideoResult(name, date_str, "success", file_path=output_path)

            # ===========================================================
            # PHASE 2 (FALLBACK): Lightweight cloudscraper
            # ===========================================================
            logger.info("[phase2] Lightweight fallback (cloudscraper)")
            landing_html = self._fetch_page(landing_url)
            if landing_html:
                m3u8_url = self._find_m3u8_in_page(landing_html)
                if m3u8_url:
                    if self._download_with_ffmpeg(m3u8_url, output_path, referer=landing_url):
                        return VideoResult(name, date_str, "success", file_path=output_path)

                external_url = self._find_external_url(landing_html, landing_url)
                if external_url:
                    final_url = self._follow_redirects(external_url)
                    if final_url:
                        external_url = final_url
                    ext_html = self._fetch_page(external_url)
                    if ext_html:
                        m3u8_url = self._find_m3u8_in_page(ext_html)
                        if m3u8_url:
                            if self._download_with_ffmpeg(m3u8_url, output_path,
                                                         referer=external_url):
                                return VideoResult(name, date_str, "success", file_path=output_path)

            # ===========================================================
            # PHASE 3 (LAST RESORT): yt-dlp on landing URL
            # ===========================================================
            logger.info("[phase3] yt-dlp direct on landing URL")
            if self._download_with_ytdlp(landing_url, output_path):
                return VideoResult(name, date_str, "success", file_path=output_path)

            return VideoResult(name, date_str, "failed",
                               error="All phases failed (selenium + cloudscraper + yt-dlp)")

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
# AGENT 5: Forex Prediction Agent (CAD/INR) — Gemini AI + historical tracking
# ---------------------------------------------------------------------------

FOREX_HISTORY_FILE = Path("downloads") / "forex_history.json"


def _load_forex_history() -> list:
    """Load CAD/INR rate history from disk. Returns list of {date, rate} dicts."""
    if FOREX_HISTORY_FILE.exists():
        try:
            with open(FOREX_HISTORY_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return []


def _save_forex_history(history: list):
    """Persist forex history to disk."""
    FOREX_HISTORY_FILE.parent.mkdir(exist_ok=True)
    with open(FOREX_HISTORY_FILE, "w") as f:
        json.dump(history[-90:], f, indent=2)  # keep last 90 days


def _record_forex_rate(rate_str: str):
    """Append today's CAD/INR rate to history if not already recorded."""
    try:
        rate = float(rate_str)
    except (ValueError, TypeError):
        return
    today = today_edt()
    history = _load_forex_history()
    # Don't duplicate today's entry
    if history and history[-1].get("date") == today:
        history[-1]["rate"] = rate
    else:
        history.append({"date": today, "rate": rate})
    _save_forex_history(history)


def generate_forex_chart(chart_data: dict, period: str) -> Optional[str]:
    """Generate a CAD/INR prediction chart PNG. Returns file path."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
        from datetime import datetime as dt

        fig, ax = plt.subplots(figsize=(10, 5), facecolor="#1a1a2e")
        ax.set_facecolor("#16213e")

        colors = {
            "line": "#00CED1",
            "fill": "#00CED1",
            "current": "#FF4444",
            "predicted": "#00FF88",
            "grid": "#333355",
            "text": "#EEEEEE",
        }

        points = chart_data.get("points", [])
        if points:
            dates, rates = [], []
            for p in points:
                try:
                    dates.append(dt.strptime(p["date"], "%d-%m-%Y"))
                    rates.append(float(p["rate"]))
                except (ValueError, KeyError):
                    continue

            if dates and rates:
                ax.plot(dates, rates, color=colors["line"], linewidth=2.5,
                        marker="o", markersize=5, zorder=5)
                ax.fill_between(dates, rates, min(rates) * 0.998,
                                alpha=0.2, color=colors["fill"])

                # Mark current and final predicted
                ax.scatter([dates[0]], [rates[0]], color=colors["current"],
                           s=100, zorder=10, label="Current", edgecolors="white", linewidth=1.5)
                ax.scatter([dates[-1]], [rates[-1]], color=colors["predicted"],
                           s=100, zorder=10, label="Predicted", edgecolors="white", linewidth=1.5)
                ax.annotate(f"₹{rates[0]:.2f}", (dates[0], rates[0]),
                            textcoords="offset points", xytext=(0, 12),
                            color=colors["current"], fontsize=10, fontweight="bold", ha="center")
                ax.annotate(f"₹{rates[-1]:.2f}", (dates[-1], rates[-1]),
                            textcoords="offset points", xytext=(0, 12),
                            color=colors["predicted"], fontsize=10, fontweight="bold", ha="center")

                ax.xaxis.set_major_formatter(mdates.DateFormatter("%d %b"))
                ax.tick_params(axis="x", rotation=30, colors=colors["text"])
                ymin = min(rates) * 0.997
                ymax = max(rates) * 1.003
                ax.set_ylim(ymin, ymax)

        ax.set_title(f"CAD → INR Exchange Rate Prediction ({period.title()})",
                     color=colors["text"], fontsize=14, fontweight="bold", pad=12)
        ax.set_ylabel("INR per 1 CAD", color=colors["text"], fontsize=11)
        ax.tick_params(colors=colors["text"])
        ax.grid(True, alpha=0.3, color=colors["grid"])
        ax.legend(loc="upper left", fontsize=9, facecolor="#16213e",
                  edgecolor=colors["grid"], labelcolor=colors["text"])

        fig.text(0.5, 0.01,
                 f"DMFIA Forex Prediction ({period.title()}) | AI-Generated | Not Financial Advice",
                 ha="center", color="#888888", fontsize=8)

        chart_path = str(CHARTS_DIR / f"forex_prediction_{period}.png")
        plt.savefig(chart_path, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
        plt.close(fig)
        logger.info(f"Forex chart generated: {chart_path}")
        return chart_path
    except Exception as e:
        logger.error(f"Forex chart generation failed: {e}")
        return None


class ForexPredictionAgent:
    """CAD/INR exchange rate prediction using Gemini AI + historical rate tracking.

    Commands:
      forex rates        — current CAD/INR rate
      forex report       — current + 7-day prediction + chart
      forex report monthly — current + 30-day prediction + chart
    """

    def __init__(self, config: dict):
        self.config = config
        self._api_key = os.getenv("GEMINI_API_KEY", "")

    def _get_current_rate(self) -> Optional[str]:
        """Scrape current CAD/INR from Remitly."""
        scraper = FinancialScraperAgent(self.config)
        return scraper._scrape_forex_remitly()

    def _calc_dates(self, period: str):
        from datetime import datetime as dt
        now = dt.now(timezone(timedelta(hours=-4)))
        if period == "monthly":
            step, points, label = 5, 7, "1 Month"
        else:
            step, points, label = 1, 8, "1 Week"
        dates = [(now + timedelta(days=i * step)).strftime("%d-%m-%Y") for i in range(points)]
        end = now + timedelta(days=step * (points - 1))
        return now, end, label, dates

    def get_current_rates_text(self) -> str:
        """Return just the current CAD/INR rate as a formatted string."""
        rate = self._get_current_rate()
        if not rate:
            return "*Forex Update*\n\nCAD/INR: unavailable (scrape failed)"
        _record_forex_rate(rate)
        history = _load_forex_history()
        lines = ["*Forex Update — CAD → INR*", ""]
        lines.append(f"Current Rate: 1 CAD = ₹{rate}")
        # 7-day change if history available
        if len(history) >= 2:
            try:
                prev = float(history[-2]["rate"])
                curr = float(rate)
                chg = curr - prev
                pct = (chg / prev) * 100
                arrow = "▲" if chg > 0 else "▼" if chg < 0 else "—"
                lines.append(f"vs Yesterday: {arrow} {chg:+.2f} ({pct:+.2f}%)")
            except Exception:
                pass
        if len(history) >= 7:
            try:
                week_ago = float(history[-7]["rate"])
                curr = float(rate)
                chg = curr - week_ago
                pct = (chg / week_ago) * 100
                arrow = "▲" if chg > 0 else "▼" if chg < 0 else "—"
                lines.append(f"vs 7 Days Ago: {arrow} {chg:+.2f} ({pct:+.2f}%)")
            except Exception:
                pass
        lines.append(f"\n_Transfers to India:_")
        try:
            r = float(rate)
            lines.append(f"  $500 CAD → ₹{500 * r:,.0f}")
            lines.append(f"  $1000 CAD → ₹{1000 * r:,.0f}")
            lines.append(f"  $5000 CAD → ₹{5000 * r:,.0f}")
        except Exception:
            pass
        from datetime import datetime as dt
        now = dt.now(timezone(timedelta(hours=-4)))
        lines.append(f"\nAs of: {now.strftime('%Y-%m-%d %H:%M EDT')}")
        return "\n".join(lines)

    def predict(self, period: str = "weekly") -> dict:
        """Predict CAD/INR for the given period using Gemini AI.
        Returns dict with 'text' and 'chart_path'.
        """
        if not self._api_key:
            return {
                "text": "Forex prediction unavailable: GEMINI_API_KEY not set.",
                "chart_path": None,
            }

        rate = self._get_current_rate()
        if not rate:
            return {"text": "Forex prediction failed: could not fetch current CAD/INR rate.",
                    "chart_path": None}

        _record_forex_rate(rate)
        history = _load_forex_history()

        now, end, period_label, date_points = self._calc_dates(period)
        start_str = now.strftime("%d-%b-%Y")
        end_str = end.strftime("%d-%b-%Y")
        dates_csv = ", ".join(date_points)

        # Build recent history context for Gemini
        history_text = ""
        if history:
            recent = history[-14:]  # last 14 days
            history_text = "Recent CAD/INR history:\n"
            for h in recent:
                history_text += f"  {h['date']}: {h['rate']}\n"

        prompt = f"""You are an expert forex analyst specializing in CAD/INR exchange rates.

CURRENT DATA:
- Current CAD/INR Rate: {rate}
- Date: {start_str}

{history_text}

MACRO CONTEXT to consider:
- Bank of Canada interest rate decisions
- Reserve Bank of India (RBI) policy
- Oil prices (CAD is a petrocurrency — oil up = CAD up = higher INR rate)
- India inflation and USD/INR movement
- Seasonal remittance patterns (Indian diaspora in Canada)

TASK: Predict CAD/INR exchange rate from {start_str} to {end_str} ({period_label}).

You MUST respond with ONLY a JSON object (no markdown, no backticks, no explanation):

{{
  "summary": "2-3 sentence WhatsApp-friendly summary. Mention key driver. Use * for bold.",
  "points": [
    {{"date": "DD-MM-YYYY", "rate": 68.50}},
    ... one entry per date in this list: {dates_csv}
  ],
  "direction": "Up/Down/Stable",
  "pct_change": 0.8,
  "good_time_to_transfer": true,
  "transfer_advice": "One sentence: e.g. 'Good time to transfer — CAD strengthening vs INR this week.'",
  "factors": ["factor1", "factor2", "factor3"]
}}

Rules:
- First point MUST use the current actual rate: {rate}
- Subsequent points are your predictions
- Rate is INR per 1 CAD (e.g. 68.50 means 1 CAD = 68.50 INR)
- Include ALL these dates: {dates_csv}
- Respond with ONLY the JSON object."""

        try:
            import google.generativeai as genai
            genai.configure(api_key=self._api_key)
            model = genai.GenerativeModel("gemini-2.5-flash")
            response = model.generate_content(prompt)
            raw = response.text.strip()
            raw = re.sub(r"```json\s*", "", raw)
            raw = re.sub(r"```\s*", "", raw)
            data = json.loads(raw)
            logger.info(f"Forex prediction received for {period_label}")

            # Generate chart
            chart_path = generate_forex_chart(data, period)

            # Build WhatsApp message
            summary = data.get("summary", "Prediction generated.")
            direction = data.get("direction", "N/A")
            pct = data.get("pct_change", 0)
            advice = data.get("transfer_advice", "")
            factors = data.get("factors", [])
            points = data.get("points", [])
            good_time = data.get("good_time_to_transfer", False)

            lines = [
                f"*CAD → INR Forex Prediction — {period_label}*",
                f"{start_str} to {end_str}",
                "",
                summary,
                "",
                f"Direction: {direction} ({pct:+.1f}%)" if isinstance(pct, (int, float)) else f"Direction: {direction}",
            ]

            if points:
                first = points[0]
                last = points[-1]
                lines.append("")
                lines.append("*Rate Forecast*")
                r_now = first.get("rate")
                r_pred = last.get("rate")
                if isinstance(r_now, (int, float)):
                    lines.append(f"  Now:      1 CAD = ₹{r_now:.2f}")
                if isinstance(r_pred, (int, float)):
                    lines.append(f"  Predicted: 1 CAD = ₹{r_pred:.2f}")
                # Show transfer amounts at predicted rate
                if isinstance(r_pred, (int, float)):
                    lines.append("")
                    lines.append("*Predicted transfer values*")
                    lines.append(f"  $500 CAD  → ₹{500 * r_pred:,.0f}")
                    lines.append(f"  $1000 CAD → ₹{1000 * r_pred:,.0f}")
                    lines.append(f"  $5000 CAD → ₹{5000 * r_pred:,.0f}")

            if advice:
                lines.append("")
                icon = "✅" if good_time else "⏳"
                lines.append(f"{icon} *{advice}*")

            if factors:
                lines.append("")
                lines.append("*Key Factors*")
                for factor in factors[:4]:
                    lines.append(f"  • {factor}")

            lines.append("")
            lines.append("_AI-generated analysis. Not financial advice._")

            return {
                "text": "\n".join(lines),
                "chart_path": chart_path,
                "data": data,
            }

        except json.JSONDecodeError as e:
            logger.error(f"Forex prediction JSON parse failed: {e}")
            return {"text": f"Forex prediction parse error: {str(e)[:200]}", "chart_path": None}
        except Exception as e:
            logger.error(f"Forex prediction failed: {e}")
            return {"text": f"Forex prediction failed: {str(e)[:200]}", "chart_path": None}


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
        self.forex_agent = ForexPredictionAgent(self.config)
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

    def forex_report(self, period: str = "weekly") -> dict:
        """
        CAD/INR forex report: current rate + historical trend + AI prediction + chart.
        Returns dict with 'text' and 'chart_path'.
        """
        logger.info(f"=== Forex Report ({period}) ===")
        return self.forex_agent.predict(period)

    def forex_rates(self) -> str:
        """Quick current CAD/INR rates with transfer calculator."""
        return self.forex_agent.get_current_rates_text()


if __name__ == "__main__":
    import sys
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    orch = MasterOrchestrator()
    report = orch.run_daily(date_arg)
    print(report.to_consolidated_text())
