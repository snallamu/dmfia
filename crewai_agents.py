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
        if "url_template" in serial_cfg:
            return serial_cfg["url_template"].replace("{date}", date_str)
        # Legacy fallback
        return serial_cfg.get("base_url", "").rstrip("/") + "/" + date_str + "/"

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

    def download_serial(self, serial_cfg: dict, date_str: str) -> VideoResult:
        name = serial_cfg["name"]
        page_url = self._build_url(serial_cfg, date_str)
        filename = f"{name}_{date_str}.mp4".replace(" ", "_")
        output_path = str(DOWNLOAD_DIR / filename)
        logger.info(f"Processing: {name} {date_str} -> {page_url}")
        try:
            if self._download_with_ytdlp(page_url, output_path):
                return VideoResult(name, date_str, "success", file_path=output_path)
            video_url = self._scrape_video_url(page_url)
            if video_url:
                if ".m3u8" in video_url:
                    if self._download_with_ffmpeg(video_url, output_path, referer=page_url):
                        return VideoResult(name, date_str, "success", file_path=output_path)
                    if self._download_with_ytdlp(video_url, output_path):
                        return VideoResult(name, date_str, "success", file_path=output_path)
                elif self._download_with_ytdlp(video_url, output_path):
                    return VideoResult(name, date_str, "success", file_path=output_path)
            m3u8_url = self._try_intercept_m3u8(page_url)
            if m3u8_url:
                if self._download_with_ffmpeg(m3u8_url, output_path, referer=page_url):
                    return VideoResult(name, date_str, "success", file_path=output_path)
                if self._download_with_ytdlp(m3u8_url, output_path):
                    return VideoResult(name, date_str, "success", file_path=output_path)
                return VideoResult(name, date_str, "failed", error="FFmpeg+yt-dlp failed on m3u8")
            return VideoResult(name, date_str, "failed", error="No m3u8 found (all methods failed)")
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
# MASTER ORCHESTRATOR
# ---------------------------------------------------------------------------

class MasterOrchestrator:
    def __init__(self, config_path: str = "config.yaml"):
        self.config = load_config(config_path)
        self.video_agent = VideoDownloaderAgent(self.config)
        self.finance_agent = FinancialScraperAgent(self.config)
        self.delivery_agent = DeliveryAgent(self.config)
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


if __name__ == "__main__":
    import sys
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    orch = MasterOrchestrator()
    report = orch.run_daily(date_arg)
    print(report.to_consolidated_text())
