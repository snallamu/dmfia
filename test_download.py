#!/usr/bin/env python3
"""
DMFIA Live Download Test
Follows the exact Selenium IDE recorded flow:
  1. Open tamildhool.tech landing page
  2. Click .td-safe-note ("Tap to watch - opens external video source")
  3. Video loads in NESTED IFRAMES on same page
  4. Enter iframe[0] -> nested iframe[0] -> JW Player
  5. Click .jw-icon-display / .jw-icon-playback
  6. Intercept m3u8 from network (coke.infamous.network/stream/...)
  7. Download with ffmpeg

Usage:
  railway run python test_download.py
  railway run python test_download.py 13-04-2026
"""
import sys, os, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from crewai_agents import VideoDownloaderAgent, load_config, today_edt

config = load_config()
agent = VideoDownloaderAgent(config)
date = sys.argv[1] if len(sys.argv) > 1 else today_edt()

serial = config["serials"][0]  # Singapenne
landing_url = agent._build_url(serial, date)

print("=" * 60)
print(f"DMFIA LIVE TEST | {serial['name']} | {date}")
print(f"Landing: {landing_url}")
print("=" * 60)
print()

# ---- FULL PIPELINE TEST ----
print("Running full download_serial pipeline...")
print("Phase 1 (PRIMARY): Selenium -> click .td-safe-note -> iframes -> JW Player -> m3u8")
print("Phase 2 (FALLBACK): cloudscraper -> parse HTML -> find m3u8")
print("Phase 3 (LAST):     yt-dlp direct")
print()

t0 = time.time()
result = agent.download_serial(serial, date)
elapsed = time.time() - t0

print()
print("=" * 60)
print(f"Status:  {result.status}")
print(f"Time:    {elapsed:.0f}s")
if result.file_path and os.path.exists(result.file_path):
    sz = os.path.getsize(result.file_path) / (1024 * 1024)
    print(f"File:    {result.file_path} ({sz:.1f} MB)")
if result.error:
    print(f"Error:   {result.error}")
print(f"RESULT:  {'SUCCESS' if result.status == 'success' else 'FAILED'}")
print("=" * 60)
