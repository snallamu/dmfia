#!/usr/bin/env python3
"""
DMFIA Live Download Test - Run on Railway or locally with deps installed.
Usage: python test_download.py
       python test_download.py 12-04-2026    # specific date
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
print(f"DMFIA LIVE DOWNLOAD TEST - {date}")
print(f"Serial:  {serial['name']}")
print(f"Landing: {landing_url}")
print("=" * 60)
print()

# ---- TEST 1: Fetch landing page ----
print("--- TEST 1: Fetch landing page ---")
t0 = time.time()
html = agent._fetch_page(landing_url)
elapsed = time.time() - t0
if html:
    print(f"  PASS: {len(html)} chars in {elapsed:.1f}s")
    if "tap to watch" in html.lower():
        print(f"  FOUND: 'Tap to watch' text in page")
    else:
        print(f"  WARN: No 'Tap to watch' text (may use different wording)")
else:
    print(f"  FAIL: Could not fetch landing page ({elapsed:.1f}s)")
    print(f"  Skipping to Phase 3 (selenium)...")
    print()
    print("--- FALLBACK: Selenium Phase 3 ---")
    t0 = time.time()
    result = agent._selenium_full_flow(landing_url)
    elapsed = time.time() - t0
    print(f"  Result: {result}")
    print(f"  Time: {elapsed:.0f}s")
    if result and not result.startswith("EXTERNAL:") and ".m3u8" in result:
        print(f"  Attempting ffmpeg download...")
        output = f"downloads/{serial['name']}_{date}_test.mp4"
        ok = agent._download_with_ffmpeg(result, output, referer=landing_url)
        print(f"  Download: {'SUCCESS' if ok else 'FAILED'}")
    sys.exit(0)
print()

# ---- TEST 2: Find external URL ----
print("--- TEST 2: Find external video link ---")
ext_url = agent._find_external_url(html, landing_url)
if ext_url:
    print(f"  PASS: {ext_url}")
else:
    print(f"  FAIL: No external link found")
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "lxml")
    links = [(a.get_text(strip=True)[:50], a["href"][:100]) for a in soup.find_all("a", href=True)]
    print(f"  All {len(links)} links:")
    for text, href in links[:15]:
        print(f"    '{text}' -> {href}")
    sys.exit(1)
print()

# ---- TEST 3: Follow redirects ----
print("--- TEST 3: Follow redirect chain ---")
t0 = time.time()
final_url = agent._follow_redirects(ext_url)
elapsed = time.time() - t0
if final_url and final_url != ext_url:
    print(f"  PASS: Redirected in {elapsed:.1f}s")
    print(f"  From: {ext_url}")
    print(f"  To:   {final_url}")
    ext_url = final_url
else:
    print(f"  INFO: No redirect (same URL) in {elapsed:.1f}s")
print()

# ---- TEST 4: Fetch external page ----
print("--- TEST 4: Fetch external page ---")
t0 = time.time()
ext_html = agent._fetch_page(ext_url)
elapsed = time.time() - t0
if ext_html:
    print(f"  PASS: {len(ext_html)} chars in {elapsed:.1f}s")
else:
    print(f"  FAIL: Could not fetch ({elapsed:.1f}s)")
    print(f"  Trying yt-dlp directly...")
    output = f"downloads/{serial['name']}_{date}_test.mp4"
    ok = agent._download_with_ytdlp(ext_url, output)
    print(f"  yt-dlp: {'SUCCESS' if ok else 'FAILED'}")
    sys.exit(0 if ok else 1)
print()

# ---- TEST 5: Find m3u8/video URL ----
print("--- TEST 5: Find m3u8/video URL in page ---")
video_url = agent._find_m3u8_in_page(ext_html)
if video_url:
    print(f"  PASS: Found video URL")
    print(f"  Type: {'m3u8/HLS' if '.m3u8' in video_url else 'MP4' if '.mp4' in video_url else 'other'}")
    print(f"  URL:  {video_url[:120]}...")
else:
    print(f"  FAIL: No video URL found in page source")
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(ext_html, "lxml")
    iframes = soup.find_all("iframe")
    scripts = soup.find_all("script")
    print(f"  Iframes: {len(iframes)}")
    for iframe in iframes[:5]:
        print(f"    src: {iframe.get('src', 'none')[:100]}")
    print(f"  Scripts: {len(scripts)}")
    for s in scripts:
        t = s.get_text()
        if any(w in t.lower() for w in [".m3u8", ".mp4", "file:", "source:", "jwplayer", "player"]):
            print(f"    Interesting ({len(t)} chars): {t[:200]}")
    print()
    print("  Trying selenium Phase 3 instead...")
    result = agent._selenium_full_flow(landing_url)
    print(f"  Selenium result: {result}")
    sys.exit(1)
print()

# ---- TEST 6: Download video ----
print("--- TEST 6: Download video ---")
output = f"downloads/{serial['name']}_{date}_test.mp4"
os.makedirs("downloads", exist_ok=True)

if ".m3u8" in video_url:
    print(f"  Using ffmpeg for HLS...")
    t0 = time.time()
    ok = agent._download_with_ffmpeg(video_url, output, referer=ext_url)
    elapsed = time.time() - t0
    if ok:
        sz = os.path.getsize(output) / (1024 * 1024)
        print(f"  PASS: {sz:.1f} MB in {elapsed:.0f}s")
    else:
        print(f"  ffmpeg failed, trying yt-dlp...")
        ok = agent._download_with_ytdlp(video_url, output)
        if ok:
            sz = os.path.getsize(output) / (1024 * 1024)
            print(f"  PASS (yt-dlp): {sz:.1f} MB")
        else:
            print(f"  FAIL: Both ffmpeg and yt-dlp failed")
elif ".mp4" in video_url:
    print(f"  Using direct download for MP4...")
    ok = agent._download_direct(video_url, output)
    if not ok:
        ok = agent._download_with_ytdlp(video_url, output)
    if ok:
        sz = os.path.getsize(output) / (1024 * 1024)
        print(f"  PASS: {sz:.1f} MB")
    else:
        print(f"  FAIL: Download failed")
else:
    print(f"  Using yt-dlp...")
    ok = agent._download_with_ytdlp(video_url, output)
    if ok:
        sz = os.path.getsize(output) / (1024 * 1024)
        print(f"  PASS: {sz:.1f} MB")
    else:
        print(f"  FAIL")

print()
print("=" * 60)
if ok:
    print("RESULT: SUCCESS")
    if os.path.exists(output):
        print(f"FILE: {output} ({os.path.getsize(output)/(1024*1024):.1f} MB)")
else:
    print("RESULT: FAILED")
print("=" * 60)
