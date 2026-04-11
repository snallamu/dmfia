"""
DMFIA - CrewAI Agents & Orchestrator
Daily Media & Financial Intelligence Agent

Revised: DeliveryAgent supports per-category WhatsApp routing.
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


def load_config(path: str = "config.yaml") -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class VideoResult:
    serial_name: str
    date_str: str
    status: str  # success / failed
    file_path: Optional[str] = None
    error: Optional[str] = None


@dataclass
class FinancialData:
    gold_22k: Optional[str] = None
    gold_24k: Optional[str] = None
    cad_to_inr: Optional[str] = None
    timestamp: Optional[str] = None
    errors: list = field(default_factory=list)

    def to_text(self) -> str:
        lines = [
            "--- FINANCIAL UPDATE ---",
            f"Gold 22k: {self.gold_22k or 'N/A'}",
            f"Gold 24k: {self.gold_24k or 'N/A'}",
            f"CAD/INR:  {self.cad_to_inr or 'N/A'}",
            f"As of:    {self.timestamp or 'N/A'}",
        ]
        if self.errors:
            lines.append(f"Warnings: {'; '.join(self.errors)}")
        return "\n".join(lines)


@dataclass
class DeliveryReceipt:
    """Tracks which messages went to which numbers and whether they succeeded."""
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
                lines.append(f"[{icon}] {r.category} -> {r.label} ({r.phone})")
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
# AGENT 1: Video Downloader (unchanged)
# ---------------------------------------------------------------------------

class VideoDownloaderAgent:
    """Downloads Tamil serial episodes using yt-dlp (primary) with
    selenium-wire HLS interception as fallback."""

    def __init__(self, config: dict):
        self.config = config
        self.servers = config.get("servers", ["server1", "server2"])

    def _build_url(self, base_url: str, date_str: str) -> str:
        return base_url.rstrip("/") + "/" + date_str + "/"

    # --- Method 1: yt-dlp (reliable, handles most video sites) ---

    def _download_with_ytdlp(self, page_url: str, output_path: str) -> bool:
        """Use yt-dlp to extract and download video from page URL."""
        logger.info(f"[yt-dlp] Attempting download from: {page_url}")
        cmd = [
            "yt-dlp",
            "--no-check-certificates",
            "--user-agent",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "--referer", page_url,
            "-f", "best[ext=mp4]/best",
            "--merge-output-format", "mp4",
            "-o", output_path,
            "--no-playlist",
            "--socket-timeout", "30",
            "--retries", "3",
            page_url,
        ]
        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=600
            )
            if result.returncode == 0 and Path(output_path).exists():
                size_mb = Path(output_path).stat().st_size / (1024 * 1024)
                logger.info(f"[yt-dlp] Download complete: {output_path} ({size_mb:.1f} MB)")
                return True
            logger.warning(f"[yt-dlp] Failed (rc={result.returncode}): {result.stderr[:300]}")
            return False
        except subprocess.TimeoutExpired:
            logger.warning("[yt-dlp] Timeout after 600s")
            return False
        except FileNotFoundError:
            logger.warning("[yt-dlp] yt-dlp binary not found")
            return False

    # --- Method 2: Selenium-wire HLS interception (fallback) ---

    def _try_intercept_m3u8(self, page_url: str) -> Optional[str]:
        logger.info(f"[selenium-wire] Intercepting HLS from: {page_url}")
        try:
            from seleniumwire import webdriver as sw_webdriver
            from selenium.webdriver.chrome.options import Options
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC

            chrome_options = Options()
            chrome_options.add_argument("--headless=new")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1920,1080")
            chrome_options.add_argument(
                "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
            )
            sw_options = {"disable_encoding": True}
            driver = sw_webdriver.Chrome(
                options=chrome_options, seleniumwire_options=sw_options
            )
            driver.set_page_load_timeout(60)
            try:
                driver.get(page_url)
                time.sleep(10)
                # Try clicking play button
                for sel in [".jw-icon-display", ".vjs-big-play-button",
                            "video", ".play-btn", "#player"]:
                    try:
                        btn = WebDriverWait(driver, 5).until(
                            EC.element_to_be_clickable((By.CSS_SELECTOR, sel))
                        )
                        btn.click()
                        time.sleep(3)
                        break
                    except Exception:
                        continue
                # Try server buttons
                for server in self.servers:
                    try:
                        btn = driver.find_element(
                            By.XPATH, f"//a[contains(text(),'{server}')]"
                        )
                        btn.click()
                        time.sleep(6)
                    except Exception:
                        continue
                    for req in driver.requests:
                        if req.response and ".m3u8" in req.url:
                            logger.info(f"[selenium-wire] Found m3u8: {req.url}")
                            return req.url
                # Final sweep of all requests
                for req in driver.requests:
                    if req.response and ".m3u8" in req.url:
                        logger.info(f"[selenium-wire] Found m3u8: {req.url}")
                        return req.url
                logger.warning("[selenium-wire] No m3u8 URL found")
                return None
            finally:
                driver.quit()
        except Exception as e:
            logger.warning(f"[selenium-wire] Failed: {e}")
            return None

    def _download_with_ffmpeg(self, m3u8_url: str, output_path: str) -> bool:
        logger.info(f"[ffmpeg] Downloading HLS to: {output_path}")
        cmd = [
            "ffmpeg", "-y",
            "-headers", "User-Agent: Mozilla/5.0\r\n",
            "-i", m3u8_url,
            "-c", "copy", "-bsf:a", "aac_adtstoasc",
            output_path,
        ]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
            if result.returncode == 0 and Path(output_path).exists():
                size_mb = Path(output_path).stat().st_size / (1024 * 1024)
                logger.info(f"[ffmpeg] Done: {output_path} ({size_mb:.1f} MB)")
                return True
            logger.error(f"[ffmpeg] Failed: {result.stderr[:500]}")
            return False
        except subprocess.TimeoutExpired:
            logger.error("[ffmpeg] Timeout after 600s")
            return False
        except FileNotFoundError:
            logger.error("[ffmpeg] ffmpeg not found")
            return False

    # --- Method 3: Direct page scraping for embedded m3u8/mp4 URLs ---

    def _scrape_video_url(self, page_url: str) -> Optional[str]:
        """Scrape the page HTML for embedded video/m3u8/mp4 URLs."""
        logger.info(f"[scrape] Looking for video URLs in: {page_url}")
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
            )
        }
        try:
            resp = requests.get(page_url, headers=headers, timeout=20)
            resp.raise_for_status()
            html = resp.text
            # Look for m3u8 URLs in page source
            m3u8_urls = re.findall(
                r'https?://[^\s\'"<>]+\.m3u8[^\s\'"<>]*', html, re.I
            )
            if m3u8_urls:
                logger.info(f"[scrape] Found m3u8: {m3u8_urls[0]}")
                return m3u8_urls[0]
            # Look for mp4 URLs
            mp4_urls = re.findall(
                r'https?://[^\s\'"<>]+\.mp4[^\s\'"<>]*', html, re.I
            )
            if mp4_urls:
                logger.info(f"[scrape] Found mp4: {mp4_urls[0]}")
                return mp4_urls[0]
            # Look for iframe sources (embedded players)
            soup = BeautifulSoup(html, "lxml")
            for iframe in soup.find_all("iframe"):
                src = iframe.get("src", "")
                if src and ("player" in src.lower() or "embed" in src.lower()):
                    logger.info(f"[scrape] Found player iframe: {src}")
                    # Fetch iframe page and look for video URLs
                    try:
                        iframe_resp = requests.get(src, headers=headers, timeout=15)
                        iframe_m3u8 = re.findall(
                            r'https?://[^\s\'"<>]+\.m3u8[^\s\'"<>]*',
                            iframe_resp.text, re.I
                        )
                        if iframe_m3u8:
                            logger.info(f"[scrape] Found m3u8 in iframe: {iframe_m3u8[0]}")
                            return iframe_m3u8[0]
                    except Exception:
                        pass
            return None
        except Exception as e:
            logger.warning(f"[scrape] Failed: {e}")
            return None

    # --- Main download pipeline ---

    def download_serial(self, serial_cfg: dict, date_str: str) -> VideoResult:
        name = serial_cfg["name"]
        page_url = self._build_url(serial_cfg["base_url"], date_str)
        filename = f"{name}_{date_str}.mp4".replace(" ", "_")
        output_path = str(DOWNLOAD_DIR / filename)
        logger.info(f"Processing serial: {name} for {date_str} -> {page_url}")

        try:
            # Strategy 1: yt-dlp (handles most sites automatically)
            if self._download_with_ytdlp(page_url, output_path):
                return VideoResult(name, date_str, "success", file_path=output_path)

            # Strategy 2: Scrape page for direct video/m3u8 URLs
            video_url = self._scrape_video_url(page_url)
            if video_url:
                if video_url.endswith(".m3u8") or ".m3u8" in video_url:
                    if self._download_with_ffmpeg(video_url, output_path):
                        return VideoResult(name, date_str, "success", file_path=output_path)
                elif self._download_with_ytdlp(video_url, output_path):
                    return VideoResult(name, date_str, "success", file_path=output_path)

            # Strategy 3: Selenium-wire HLS interception (heavy, last resort)
            m3u8_url = self._try_intercept_m3u8(page_url)
            if m3u8_url:
                if self._download_with_ffmpeg(m3u8_url, output_path):
                    return VideoResult(name, date_str, "success", file_path=output_path)
                return VideoResult(name, date_str, "failed", error="FFmpeg conversion failed")

            return VideoResult(
                name, date_str, "failed",
                error="All download methods failed (yt-dlp, scrape, selenium-wire)"
            )

        except Exception as e:
            logger.exception(f"Video download error for {name}")
            return VideoResult(name, date_str, "failed", error=str(e)[:200])

    def run(self, date_str: Optional[str] = None) -> list:
        if not date_str:
            date_str = datetime.now().strftime("%d-%m-%Y")
        return [self.download_serial(s, date_str) for s in self.config.get("serials", [])]


# ---------------------------------------------------------------------------
# AGENT 2: Financial Scraper (unchanged)
# ---------------------------------------------------------------------------

class FinancialScraperAgent:
    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
        )
    }

    def __init__(self, config: dict):
        self.config = config

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=3, max=15))
    def _fetch_page(self, url: str) -> str:
        resp = requests.get(url, headers=self.HEADERS, timeout=20)
        resp.raise_for_status()
        return resp.text

    def _scrape_gold_angelone(self) -> dict:
        url = "https://www.angelone.in/gold-rates-today"
        try:
            html = self._fetch_page(url)
            soup = BeautifulSoup(html, "lxml")
            text = soup.get_text(" ", strip=True)
            data = {}
            m24 = re.search(r"24[Kk]\w*\s*(?:gold)?\s*[:\-]?\s*(?:Rs\.?|INR)?\s*([\d,]+(?:\.\d+)?)", text, re.I)
            m22 = re.search(r"22[Kk]\w*\s*(?:gold)?\s*[:\-]?\s*(?:Rs\.?|INR)?\s*([\d,]+(?:\.\d+)?)", text, re.I)
            if m24:
                data["gold_24k"] = m24.group(1)
            if m22:
                data["gold_22k"] = m22.group(1)
            return data
        except Exception as e:
            logger.warning(f"AngelOne scrape failed: {e}")
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
            logger.warning(f"GoodReturns scrape failed: {e}")
            return {}

    def _scrape_gold_ibja(self) -> dict:
        """Scrape IBJA (India Bullion and Jewellers Association) benchmark rates.
        IBJA header shows per-gram rates: 999 purity = 24k, 916 purity = 22k.
        """
        url = "https://ibjarates.com/"
        try:
            html = self._fetch_page(url)
            soup = BeautifulSoup(html, "lxml")
            text = soup.get_text(" ", strip=True)
            data = {}
            # Pattern matches: "999 Purity ### 15033 (1 Gram)"
            # The page has multiple ### headings per purity
            all_rates = re.findall(
                r"(\d{3})\s*Purity\s*#{1,3}\s*([\d,]+)\s*\(1\s*Gram\)",
                text, re.I
            )
            for purity, rate in all_rates:
                if purity == "999" and not data.get("gold_24k"):
                    data["gold_24k"] = rate
                elif purity == "916" and not data.get("gold_22k"):
                    data["gold_22k"] = rate
            if data:
                logger.info(f"IBJA gold rates: {data}")
            return data
        except Exception as e:
            logger.warning(f"IBJA scrape failed: {e}")
            return {}

    def _scrape_forex_remitly(self) -> Optional[str]:
        url = "https://www.remitly.com/ca/en/currency-converter/cad-to-inr-rate"
        try:
            html = self._fetch_page(url)
            soup = BeautifulSoup(html, "lxml")
            text = soup.get_text(" ", strip=True)
            match = re.search(r"1\s*CAD\s*=?\s*([\d.]+)\s*INR", text, re.I)
            if match:
                return match.group(1)
            match2 = re.search(r"([\d]{2}\.[\d]+)\s*(?:INR|Indian)", text)
            if match2:
                return match2.group(1)
            return None
        except Exception as e:
            logger.warning(f"Remitly scrape failed: {e}")
            return None

    def run(self) -> FinancialData:
        result = FinancialData()
        now = datetime.now(timezone(timedelta(hours=-4)))
        result.timestamp = now.strftime("%Y-%m-%d %H:%M EDT")
        gold = self._scrape_gold_ibja()
        if not gold.get("gold_24k"):
            gold2 = self._scrape_gold_angelone()
            gold = {**gold, **gold2}
        if not gold.get("gold_24k"):
            gold3 = self._scrape_gold_goodreturns()
            gold = {**gold, **gold3}
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
# AGENT 3: Delivery Agent - MULTI-TARGET WhatsApp Routing
# ---------------------------------------------------------------------------

class DeliveryAgent:
    """
    Sends messages to different WhatsApp numbers via Twilio API.

    Config structure (config.yaml):
        whatsapp_targets:
          videos:
            - phone: "+16473386458"
              label: "Sn (primary)"
          financial:
            - phone: "+16473386458"
              label: "Sn (primary)"
          consolidated_report:
            - phone: "+16473386458"
              label: "Sn (primary)"
    """

    CAT_VIDEOS = "videos"
    CAT_FINANCIAL = "financial"
    CAT_REPORT = "consolidated_report"

    def __init__(self, config: dict):
        self.targets = config.get("whatsapp_targets", {})
        if not self.targets:
            fallback = os.getenv("PHONE", config.get("phone", ""))
            if fallback:
                entry = [{"phone": fallback, "label": "default"}]
                self.targets = {
                    self.CAT_VIDEOS: list(entry),
                    self.CAT_FINANCIAL: list(entry),
                    self.CAT_REPORT: list(entry),
                }
        self._twilio_sid = os.getenv("TWILIO_ACCOUNT_SID", "")
        self._twilio_token = os.getenv("TWILIO_AUTH_TOKEN", "")
        self._twilio_from = os.getenv(
            "TWILIO_WHATSAPP_NUMBER", "whatsapp:+14155238886"
        )

    def _get_twilio_client(self):
        if not self._twilio_sid or not self._twilio_token:
            logger.error("Twilio credentials not set. Cannot deliver.")
            return None
        try:
            from twilio.rest import Client
            return Client(self._twilio_sid, self._twilio_token)
        except Exception as e:
            logger.error(f"Twilio client init failed: {e}")
            return None

    def _get_phones(self, category: str) -> list:
        return self.targets.get(category, [])

    def _send_text_to(self, phone: str, message: str) -> bool:
        if not phone:
            return False
        client = self._get_twilio_client()
        if not client:
            return False
        try:
            to_addr = phone if phone.startswith("whatsapp:") else f"whatsapp:{phone}"
            msg = client.messages.create(
                from_=self._twilio_from,
                to=to_addr,
                body=message,
            )
            logger.info(f"WhatsApp text sent to {phone}: SID={msg.sid}")
            return True
        except Exception as e:
            logger.error(f"WhatsApp text to {phone} failed: {e}")
            return False

    def _send_file_to(self, phone: str, file_path: str, caption: str = "") -> bool:
        """Send a file via Twilio. Note: Twilio requires publicly accessible URLs
        for media. For local files, we send a text notification instead."""
        if not phone or not Path(file_path).exists():
            return False
        size_mb = Path(file_path).stat().st_size / (1024 * 1024)
        msg = f"{caption}\n\nFile: {Path(file_path).name} ({size_mb:.1f} MB)"
        msg += "\n(File downloaded on server, ready for pickup)"
        return self._send_text_to(phone, msg)

    # --- Public category-based methods ---

    def send_videos(self, video_results: list) -> list:
        receipts = []
        targets = self._get_phones(self.CAT_VIDEOS)
        if not targets:
            logger.warning("No WhatsApp targets for 'videos' category")
            return receipts

        for vr in video_results:
            if vr.status != "success" or not vr.file_path:
                continue
            caption = f"{vr.serial_name} - {vr.date_str}"
            for t in targets:
                phone = t.get("phone", "")
                label = t.get("label", phone)
                ok = self._send_file_to(phone, vr.file_path, caption)
                receipts.append(DeliveryReceipt(
                    category=self.CAT_VIDEOS,
                    phone=phone, label=label, success=ok,
                    error=None if ok else f"Failed sending {vr.serial_name}",
                ))
        return receipts

    def send_financial(self, financial: FinancialData) -> list:
        receipts = []
        targets = self._get_phones(self.CAT_FINANCIAL)
        if not targets:
            logger.warning("No WhatsApp targets for 'financial' category")
            return receipts

        msg = financial.to_text()
        for t in targets:
            phone = t.get("phone", "")
            label = t.get("label", phone)
            ok = self._send_text_to(phone, msg)
            receipts.append(DeliveryReceipt(
                category=self.CAT_FINANCIAL,
                phone=phone, label=label, success=ok,
                error=None if ok else "Send failed",
            ))
        return receipts

    def send_consolidated_report(self, report_text: str) -> list:
        receipts = []
        targets = self._get_phones(self.CAT_REPORT)
        if not targets:
            logger.warning("No WhatsApp targets for 'consolidated_report' category")
            return receipts

        for t in targets:
            phone = t.get("phone", "")
            label = t.get("label", phone)
            ok = self._send_text_to(phone, report_text)
            receipts.append(DeliveryReceipt(
                category=self.CAT_REPORT,
                phone=phone, label=label, success=ok,
                error=None if ok else "Send failed",
            ))
        return receipts

    def disable_all(self):
        self.targets = {}


# ---------------------------------------------------------------------------
# MASTER ORCHESTRATOR (revised delivery phase)
# ---------------------------------------------------------------------------

class MasterOrchestrator:
    """Coordinates all agents and routes deliveries per category."""

    def __init__(self, config_path: str = "config.yaml"):
        self.config = load_config(config_path)
        self.video_agent = VideoDownloaderAgent(self.config)
        self.finance_agent = FinancialScraperAgent(self.config)
        self.delivery_agent = DeliveryAgent(self.config)
        self.reports: list = []

    def run_daily(self, date_str: Optional[str] = None) -> DailyReport:
        if not date_str:
            date_str = datetime.now().strftime("%d-%m-%Y")
        logger.info(f"=== DMFIA Daily Run: {date_str} ===")

        report = DailyReport(date=date_str)

        # Phase 1: Video downloads
        logger.info("--- Phase 1: Video Downloads ---")
        try:
            report.video_results = self.video_agent.run(date_str)
        except Exception as e:
            logger.exception("Video agent crashed")
            report.video_results = [
                VideoResult("ALL", date_str, "failed", error=f"Agent crash: {str(e)[:100]}")
            ]

        # Phase 2: Financial data
        logger.info("--- Phase 2: Financial Data ---")
        try:
            report.financial = self.finance_agent.run()
        except Exception as e:
            logger.exception("Finance agent crashed")
            report.financial = FinancialData(errors=[f"Agent crash: {str(e)[:100]}"])

        # Phase 3: Multi-target delivery
        logger.info("--- Phase 3: Delivery (multi-target) ---")
        try:
            # 3a. Videos -> videos targets
            report.delivery_receipts.extend(
                self.delivery_agent.send_videos(report.video_results)
            )

            # 3b. Financial -> financial targets
            if report.financial:
                report.delivery_receipts.extend(
                    self.delivery_agent.send_financial(report.financial)
                )

            # 3c. Consolidated report -> report targets
            report.delivery_receipts.extend(
                self.delivery_agent.send_consolidated_report(report.to_consolidated_text())
            )

        except Exception as e:
            logger.exception("Delivery agent crashed")
            report.delivery_receipts.append(
                DeliveryReceipt("system", "", "N/A", False, f"Agent crash: {str(e)[:100]}")
            )

        # Persist report
        report_file = DOWNLOAD_DIR / f"report_{date_str}.json"
        with open(report_file, "w") as f:
            json.dump(report.to_dict(), f, indent=2)
        logger.info(f"Report saved: {report_file}")

        self.reports.append(report)
        return report


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    orchestrator = MasterOrchestrator()
    report = orchestrator.run_daily(date_arg)
    print(report.to_consolidated_text())
