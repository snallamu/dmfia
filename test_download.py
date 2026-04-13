#!/usr/bin/env python3
"""
DMFIA Live Download Test - Run this on Railway to test actual downloads.
Usage: python test_download.py
"""
import sys
import os
import time

# Add project to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from crewai_agents import VideoDownloaderAgent, load_config, today_edt

config = load_config()
agent = VideoDownloaderAgent(config)
date = today_edt()

print("=" * 60)
print(f"DMFIA LIVE DOWNLOAD TEST - {date}")
print("=" * 60)
print()

serial = config["serials"][0]  # Singapenne
landing_url = agent._build_url(serial, date)
print(f"Serial: {serial['name']}")
print(f"Date:   {date}")
print(f"URL:    {landing_url}")
print()

# ---- TEST 1: Fetch landing page ----
print("--- TEST 1: Fetch landing page ---")
t0 = time.time()
html = agent._fetch_page(landing_url)
t1 = time.time()
if html:
    print(f"  PASS: {len(html)} chars in {t1-t0:.1f}s")
else:
    print(f"  FAIL: Could not fetch ({t1-t0:.1f}s)")
    print("  Trying curl directly...")
    import subprocess
    r = subprocess.run(["curl", "-sL", "-o", "/dev/null", "-w", "%{http_code}",
                        "-A", "Mozilla/5.0", landing_url],
                       capture_output=True, text=True, timeout=30)
    print(f"  curl status: {r.stdout}")
print()

if not html:
    print("Cannot proceed without landing page. Testing selenium instead...")
    print()
    print("--- TEST SELENIUM DIRECT ---")
    result = agent._selenium_full_flow(landing_url)
    print(f"  Result: {result}")
    sys.exit(1)

# ---- TEST 2: Find external URL ----
print("--- TEST 2: Find 'Tap to watch' link ---")
ext_url = agent._find_external_url(html, landing_url)
if ext_url:
    print(f"  PASS: {ext_url}")
else:
    print(f"  FAIL: No external link found")
    # Show what links exist
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "lxml")
    links = soup.find_all("a", href=True)
    print(f"  Found {len(links)} links:")
    for a in links[:10]:
        print(f"    {a.get_text(strip=True)[:40]:40s} -> {a['href'][:80]}")
print()

if not ext_url:
    print("No external URL. Testing selenium Phase 3...")
    result = agent._selenium_full_flow(landing_url)
    print(f"  Selenium result: {result}")
    sys.exit(1)

# ---- TEST 3: Fetch external page ----
print("--- TEST 3: Fetch external page ---")
t0 = time.time()
ext_html = agent._fetch_page(ext_url)
t1 = time.time()
if ext_html:
    print(f"  PASS: {len(ext_html)} chars in {t1-t0:.1f}s")
else:
    print(f"  FAIL: Could not fetch external page ({t1-t0:.1f}s)")
print()

if ext_html:
    # ---- TEST 4: Find m3u8/video URL ----
    print("--- TEST 4: Find m3u8/video URL ---")
    video_url = agent._find_m3u8_in_page(ext_html)
    if video_url:
        print(f"  PASS: {video_url[:120]}")
    else:
        print(f"  FAIL: No video URL in external page")
        # Show iframes
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(ext_html, "lxml")
        iframes = soup.find_all("iframe")
        print(f"  Iframes found: {len(iframes)}")
        for iframe in iframes:
            print(f"    src: {iframe.get('src', 'none')[:100]}")
        # Show scripts with 'file' or 'source'
        scripts = soup.find_all("script")
        for s in scripts:
            text = s.get_text()
            if any(w in text.lower() for w in ["file:", "source:", ".m3u8", ".mp4"]):
                print(f"  Interesting script ({len(text)} chars): {text[:200]}")
    print()

# ---- TEST 5: Full download attempt ----
print("--- TEST 5: Full download_serial ---")
print("This may take several minutes...")
t0 = time.time()
result = agent.download_serial(serial, date)
t1 = time.time()
print(f"  Status: {result.status}")
print(f"  Time:   {t1-t0:.0f}s")
if result.file_path:
    sz = os.path.getsize(result.file_path) / (1024 * 1024)
    print(f"  File:   {result.file_path} ({sz:.1f} MB)")
if result.error:
    print(f"  Error:  {result.error}")

print()
print("=" * 60)
print(f"RESULT: {'SUCCESS' if result.status == 'success' else 'FAILED'}")
print("=" * 60)
