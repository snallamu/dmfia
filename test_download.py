#!/usr/bin/env python3
"""
DMFIA Self-Healing Download Test
Retries with different strategies until video downloads successfully.

Install deps first:
  pip install selenium python-dotenv pyyaml requests beautifulsoup4 lxml cloudscraper tenacity yt-dlp

Usage:
  python test_download.py              # today, unlimited retries
  python test_download.py 14-04-2026   # specific date
  python test_download.py 14-04-2026 3 # max 3 attempts
"""
import sys, os, time, shutil, traceback

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# ---- Dep check ----
missing = []
for mod_name, pkg in [("selenium", "selenium"), ("yaml", "pyyaml"),
                       ("bs4", "beautifulsoup4"), ("dotenv", "python-dotenv")]:
    try:
        __import__(mod_name)
    except ImportError:
        missing.append(pkg)
if missing:
    print(f"Missing: {', '.join(missing)}")
    print(f"Run: pip install {' '.join(missing)}")
    sys.exit(1)

if not shutil.which("ffmpeg"):
    print("WARNING: ffmpeg not in PATH. Download: https://www.gyan.dev/ffmpeg/builds/")

from crewai_agents import VideoDownloaderAgent, load_config, today_edt
from datetime import datetime, timezone, timedelta

config = load_config()
agent = VideoDownloaderAgent(config)

# Default: yesterday (current date - 1) since today's episode may not be uploaded yet
if len(sys.argv) > 1:
    date = sys.argv[1]
else:
    yesterday = datetime.now(timezone(timedelta(hours=-4))) - timedelta(days=1)
    date = yesterday.strftime("%d-%m-%Y")
max_attempts = int(sys.argv[2]) if len(sys.argv) > 2 else 3

serial = config["serials"][0]  # Singapenne
landing_url = agent._build_url(serial, date)

print("=" * 60)
print(f"DMFIA SELF-HEALING TEST | {serial['name']} | {date}")
print(f"URL: {landing_url}")
print(f"Max attempts: {max_attempts}")
print("=" * 60)

attempt = 0
success = False

while attempt < max_attempts and not success:
    attempt += 1
    print(f"\n{'='*60}")
    print(f"ATTEMPT {attempt}")
    print(f"{'='*60}")

    try:
        t0 = time.time()
        result = agent.download_serial(serial, date)
        elapsed = time.time() - t0

        if result.status == "success":
            success = True
            sz = os.path.getsize(result.file_path) / (1024*1024) if result.file_path and os.path.exists(result.file_path) else 0
            print(f"\n*** SUCCESS on attempt {attempt} ***")
            print(f"File: {result.file_path} ({sz:.1f} MB)")
            print(f"Time: {elapsed:.0f}s")
        else:
            print(f"\nFAILED (attempt {attempt}): {result.error}")
            print(f"Time: {elapsed:.0f}s")

            if attempt < max_attempts:
                wait = min(10 * attempt, 60)
                print(f"Retrying in {wait}s...")
                time.sleep(wait)

    except Exception as e:
        print(f"\nCRASH (attempt {attempt}): {e}")
        traceback.print_exc()
        if attempt < max_attempts:
            time.sleep(15)

print(f"\n{'='*60}")
print(f"FINAL: {'SUCCESS' if success else 'FAILED'} after {attempt} attempt(s)")
print(f"{'='*60}")
sys.exit(0 if success else 1)
