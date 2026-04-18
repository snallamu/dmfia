# DMFIA — What Changed in This Build

## Bug Fixes — Video Download

### Bug 1 (CRITICAL): Path Doubling → ffmpeg crash
**Symptom:**
```
Impossible to open 'downloads\segments\downloads/segments/seg_00000.ts'
```
**Root cause:** `seg_files` stored `str(path.resolve())` (already absolute),
then `Path(str).resolve().as_posix()` was called again on it, doubling
the path on Windows/cross-platform.

**Fix:** `seg_files` now stores `Path` objects. `.resolve()` is called
exactly once, directly on the `Path` object when writing to ffmpeg.

---

### Bug 2: Downloading 131 segments instead of 1
**Symptom:** Download took 4+ minutes per serial (131 segments × ~2s each),
Railway thread timed out.

**Fix:** Now downloads **only the first `.ts` segment** (≈ 30s of video,
proves the pipeline). No concat needed — single segment converts directly
with `ffmpeg -i seg.ts -c copy output.mp4`.

---

### Bug 3: Wrong quality selected (highest → most segments)
**Symptom:** Code picked the highest-bandwidth sub-playlist from the HLS
master manifest, which has the most `.ts` segments.

**Fix:** Picks the **lowest bandwidth** (360p) stream — fewer segments,
same content, dramatically faster.

---

## New Feature — Forex Prediction (CAD → INR)

### New agent: `ForexPredictionAgent`
- Scrapes live CAD/INR from Remitly (already in FinancialScraper)
- Persists daily rates in `downloads/forex_history.json` (up to 90 days)
- Feeds Gemini AI with rate history + macro context (oil, BoC, RBI)
- Generates dark-themed CAD/INR prediction chart PNG

### New commands
| Command | Intent | Response |
|---|---|---|
| `forex rates` | `forex_rates` | Live rate + transfer calculator ($500/$1000/$5000) |
| `forex report` | `forex_report` | 7-day AI prediction + chart |
| `forex report monthly` | `forex_report` + `period=monthly` | 30-day prediction + chart |
| `good time to transfer?` | `forex_report` | Prediction + transfer advice |

### Charts
- Saved to `charts/forex_prediction_weekly.png` / `charts/forex_prediction_monthly.png`
- Served via `/charts/<filename>` endpoint same as gold charts
- Dark theme matching gold charts

### History tracking
- `downloads/forex_history.json` — auto-appended each time rates are fetched
- Used by Gemini for trend context
- Shows yesterday/7-day change in `forex rates` command

---

## What Was NOT Changed
- Gold report / gold prediction — unchanged, working
- Financial scraper — unchanged, working
- Delivery agent — unchanged, working
- Scheduler — unchanged, working
- Railway deploy config — unchanged
