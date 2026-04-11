"""
DMFIA - Streamlit Dashboard v3
Matches the React mockup: pipeline bar, agent architecture,
bento cards, WhatsApp chat preview, activity timeline.
"""

import streamlit as st
import yaml
import json
from pathlib import Path
from datetime import datetime
from crewai_agents import MasterOrchestrator, load_config

st.set_page_config(page_title="DMFIA", page_icon="📡", layout="wide")

# ---------------------------------------------------------------------------
# Design system CSS
# ---------------------------------------------------------------------------

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500&display=swap');

:root {
    --bg1: #ffffff; --bg2: #f5f5f0; --bg3: #eeeee8;
    --tx1: #2C2C2A; --tx2: #5F5E5A; --tx3: #888780;
    --bd: #D3D1C7; --bd2: #B4B2A9;
    --ok: #0F6E56; --okbg: #E1F5EE; --oktx: #085041;
    --fail: #A32D2D; --failbg: #FCEBEB; --failtx: #791F1F;
    --warn: #BA7517; --warnbg: #FAEEDA; --warntx: #633806;
    --info: #185FA5; --infobg: #E6F1FB; --infotx: #0C447C;
    --purple: #534AB7; --purplebg: #EEEDFE; --purpletx: #3C3489;
    --coral: #D85A30; --coralbg: #FAECE7; --coraltx: #712B13;
    --teal: #0F6E56; --tealbg: #E1F5EE; --tealtx: #085041;
    --amber: #BA7517; --amberbg: #FAEEDA; --ambertx: #633806;
    --blue: #185FA5; --bluebg: #E6F1FB; --bluetx: #0C447C;
    --mono: 'JetBrains Mono', monospace;
    --radius: 10px;
}

@media (prefers-color-scheme: dark) {
    :root {
        --bg1: #1a1a18; --bg2: #242422; --bg3: #2C2C2A;
        --tx1: #F1EFE8; --tx2: #B4B2A9; --tx3: #888780;
        --bd: #444441; --bd2: #5F5E5A;
    }
}

.stApp { background: var(--bg2) !important; }
.stApp [data-testid="stHeader"] { background: transparent !important; }
.stApp [data-testid="stSidebar"] { background: var(--bg1) !important; border-right: 0.5px solid var(--bd) !important; }

/* Header */
.dmfia-header { display: flex; justify-content: space-between; align-items: flex-start; flex-wrap: wrap; gap: 8px; margin-bottom: 20px; }
.dmfia-brand { display: flex; align-items: center; gap: 10px; }
.dmfia-dot { width: 8px; height: 8px; border-radius: 50%; background: var(--ok); }
.dmfia-name { font-size: 18px; font-weight: 500; color: var(--tx1); }
.dmfia-ver { font-size: 12px; color: var(--tx3); font-family: var(--mono); }
.dmfia-sub { font-size: 13px; color: var(--tx3); margin-top: 2px; margin-left: 18px; }

/* Pipeline bar */
.pipe-bar { display: flex; border: 0.5px solid var(--bd); border-radius: var(--radius); overflow: hidden; margin-bottom: 24px; }
.pipe-stage { flex: 1; padding: 12px 10px; display: flex; align-items: center; gap: 8px; background: var(--bg1); border-right: 0.5px solid var(--bd); position: relative; min-width: 0; }
.pipe-stage:last-child { border-right: none; }
.pipe-icon { width: 30px; height: 30px; border-radius: 8px; display: flex; align-items: center; justify-content: center; flex-shrink: 0; }
.pipe-label { font-size: 12px; font-weight: 500; color: var(--tx1); }
.pipe-sub { font-size: 11px; color: var(--tx3); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.pipe-check { margin-left: auto; flex-shrink: 0; }
.pipe-chevron { position: absolute; right: -8px; top: 50%; transform: translateY(-50%); z-index: 1;
    width: 16px; height: 16px; border-radius: 50%; background: var(--bg1); border: 0.5px solid var(--bd);
    display: flex; align-items: center; justify-content: center; }

/* Metric cards */
.mc { background: var(--bg2); border-radius: 8px; padding: 14px; }
.mc-label { font-size: 11px; color: var(--tx3); letter-spacing: 0.04em; text-transform: uppercase; margin-bottom: 4px; }
.mc-value { font-size: 22px; font-weight: 500; font-family: var(--mono); color: var(--tx1); }
.mc-sub { font-size: 11px; color: var(--tx3); margin-top: 4px; }

/* Cards */
.crd { background: var(--bg1); border: 0.5px solid var(--bd); border-radius: var(--radius); padding: 16px 20px; margin-bottom: 12px; }
.crd-title { font-size: 14px; font-weight: 500; color: var(--tx1); margin-bottom: 12px; }

/* Badge */
.badge { font-size: 11px; font-weight: 500; padding: 2px 10px; border-radius: 8px; display: inline-flex; align-items: center; gap: 4px; }
.badge-ok { background: var(--okbg); color: var(--oktx); }
.badge-fail { background: var(--failbg); color: var(--failtx); }
.badge-warn { background: var(--warnbg); color: var(--warntx); }
.badge-purple { background: var(--purplebg); color: var(--purpletx); }
.badge-coral { background: var(--coralbg); color: var(--coraltx); }
.badge-amber { background: var(--amberbg); color: var(--ambertx); }
.badge-blue { background: var(--bluebg); color: var(--bluetx); }
.badge-teal { background: var(--tealbg); color: var(--tealtx); }

/* Agent cards */
.agent-card { background: var(--bg1); border: 0.5px solid var(--bd); border-radius: var(--radius); padding: 16px 20px; transition: border-color 0.15s; }
.agent-card:hover { border-color: var(--bd2); }
.agent-header { display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 10px; }
.agent-icon { width: 36px; height: 36px; border-radius: 8px; display: flex; align-items: center; justify-content: center; }
.agent-stats { display: flex; gap: 8px; margin: 10px 0; flex-wrap: wrap; }
.agent-stat { flex: 1 1 70px; padding: 8px 10px; background: var(--bg2); border-radius: 8px; min-width: 0; }
.agent-stat-label { font-size: 11px; color: var(--tx3); }
.agent-stat-value { font-size: 14px; font-weight: 500; font-family: var(--mono); color: var(--tx1); }
.prog-track { height: 4px; border-radius: 2px; background: var(--bg3); overflow: hidden; margin: 10px 0 8px; }
.prog-fill { height: 100%; border-radius: 2px; transition: width 0.8s ease; }
.dep-pill { font-size: 11px; padding: 2px 8px; border-radius: 8px; background: var(--bg2); color: var(--tx3); border: 0.5px solid var(--bd); display: inline-block; margin: 2px 2px; }

/* Timeline */
.tl-row { display: flex; gap: 12px; padding-bottom: 14px; position: relative; }
.tl-dot { width: 8px; height: 8px; border-radius: 50%; margin-top: 5px; flex-shrink: 0; }
.tl-line { position: absolute; left: 3.5px; top: 14px; bottom: 0; width: 1px; background: var(--bd); }
.tl-row:last-child .tl-line { display: none; }
.tl-text { font-size: 13px; color: var(--tx1); flex: 1; }
.tl-time { font-size: 11px; font-family: var(--mono); color: var(--tx3); flex-shrink: 0; }

/* WhatsApp */
.wa-wrap { background: var(--bg2); border-radius: var(--radius); padding: 14px; max-height: 460px; overflow-y: auto; }
.wa-bub { max-width: 85%; padding: 8px 12px; border-radius: 8px; font-size: 13px; line-height: 1.55; white-space: pre-wrap; margin-bottom: 6px; }
.wa-bot { background: var(--bg1); border: 0.5px solid var(--bd); }
.wa-usr { background: var(--tealbg); color: var(--tealtx); margin-left: auto; }
.wa-time { text-align: right; font-size: 11px; color: var(--tx3); margin-top: 3px; }
.wa-route { font-size: 11px; color: var(--tx3); margin-bottom: 4px; }

/* Receipt row */
.rcpt { display: flex; align-items: center; gap: 8px; padding: 7px 0; border-top: 0.5px solid var(--bd); font-size: 13px; }
.rcpt:first-child { border-top: none; }
.rcpt-dot { width: 6px; height: 6px; border-radius: 50%; flex-shrink: 0; }
.rcpt-cat { color: var(--tx2); min-width: 70px; }
.rcpt-phone { font-family: var(--mono); font-size: 12px; flex: 1; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.rcpt-label { font-size: 12px; color: var(--tx3); flex-shrink: 0; }

/* Target row */
.tgt-row { display: flex; align-items: center; gap: 8px; padding: 6px 10px; background: var(--bg2); border-radius: 8px; margin-bottom: 3px; font-size: 13px; }
.tgt-phone { font-family: var(--mono); flex: 1; color: var(--tx1); }
.tgt-label { font-size: 12px; color: var(--tx3); }

/* Animated pulse */
@keyframes livePulse { 0%,100% { opacity:1; } 50% { opacity:0.3; } }
.live-dot { width: 6px; height: 6px; border-radius: 50%; display: inline-block; animation: livePulse 2s ease-in-out infinite; }

/* Architecture SVG animations */
@keyframes dashFlow { to { stroke-dashoffset: -20; } }
.flow-line { stroke-dasharray: 6,4; animation: dashFlow 1.2s linear infinite; }

/* Responsive */
@media (max-width: 768px) {
    .pipe-bar { flex-direction: column; }
    .pipe-stage { border-right: none !important; border-bottom: 0.5px solid var(--bd); }
    .pipe-stage:last-child { border-bottom: none; }
    .pipe-chevron { display: none; }
}
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

CONFIG_PATH = "config.yaml"
if Path(CONFIG_PATH).exists():
    config = load_config(CONFIG_PATH)
else:
    config = {"serials": [], "gold_sites": [], "forex_sites": [],
              "schedule_utc": "04:00", "whatsapp_targets": {"videos": [], "financial": [], "consolidated_report": []}}

# ---------------------------------------------------------------------------
# Load report data
# ---------------------------------------------------------------------------

downloads = Path("downloads")
reports = sorted(downloads.glob("report_*.json"), reverse=True) if downloads.exists() else []
has_report = len(reports) > 0

if has_report:
    with open(reports[0]) as f:
        latest = json.load(f)
else:
    latest = {
        "date": datetime.now().strftime("%d-%m-%Y"),
        "video_results": [
            {"serial_name": "Singapenne", "date_str": "10-04-2026", "status": "success", "file_path": "Singapenne_10-04-2026.mp4"},
            {"serial_name": "Annam", "date_str": "10-04-2026", "status": "success", "file_path": "Annam_10-04-2026.mp4"},
        ],
        "financial": {"gold_22k": "13,702", "gold_24k": "14,947", "cad_to_inr": "60.50", "timestamp": "2026-04-10 20:00 EDT", "errors": []},
        "delivery_status": "sent",
        "delivery_receipts": [
            {"category": "videos", "phone": "+16473386458", "label": "Sn", "success": True},
            {"category": "videos", "phone": "+19055551234", "label": "Mom", "success": True},
            {"category": "financial", "phone": "+16473386458", "label": "Sn", "success": True},
            {"category": "report", "phone": "+16473386458", "label": "Sn", "success": True},
            {"category": "report", "phone": "+19055551234", "label": "Mom", "success": True},
        ],
    }

fin = latest.get("financial", {})
receipts = latest.get("delivery_receipts", [])
ok_receipts = sum(1 for r in receipts if r.get("success"))

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

st.markdown(f"""
<div class="dmfia-header">
    <div>
        <div class="dmfia-brand">
            <div class="dmfia-dot"></div>
            <span class="dmfia-name">DMFIA</span>
            <span class="dmfia-ver">v2.1</span>
        </div>
        <div class="dmfia-sub">Last run completed at 04:03 UTC</div>
    </div>
    <div style="text-align:right">
        <div style="font-family:var(--mono);font-size:14px;color:var(--tx1)">{datetime.now().strftime("%H:%M:%S")}</div>
        <div style="font-size:12px;color:var(--tx3)">{datetime.now().strftime("%a, %b %d %Y")}</div>
    </div>
</div>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------

tab1, tab2, tab3, tab4, tab5 = st.tabs(["Overview", "Agents", "WhatsApp bot", "Activity", "Settings"])

# ===== TAB 1: OVERVIEW =====
with tab1:

    # Pipeline bar
    st.markdown("""
    <div class="pipe-bar">
        <div class="pipe-stage">
            <div class="pipe-icon" style="background:var(--purplebg)">
                <svg width="14" height="14" viewBox="0 0 24 24" fill="var(--purple)"><path d="M12,2A10,10 0 1,0 22,12A10,10 0 0,0 12,2M12.5,7v5.25l4.5,2.67-.75,1.23L11,13V7Z"/></svg>
            </div>
            <div><div class="pipe-label">Schedule</div><div class="pipe-sub">04:00 UTC</div></div>
            <svg class="pipe-check" width="14" height="14" viewBox="0 0 24 24" fill="var(--purple)"><path d="M9,20.42L2.79,14.21L5.62,11.38L9,14.77L18.88,4.88L21.71,7.71L9,20.42Z"/></svg>
            <div class="pipe-chevron"><svg width="8" height="8" viewBox="0 0 24 24" fill="var(--tx3)"><path d="M8.59,16.58L13.17,12L8.59,7.41L10,6L16,12L10,18Z"/></svg></div>
        </div>
        <div class="pipe-stage">
            <div class="pipe-icon" style="background:var(--coralbg)">
                <svg width="14" height="14" viewBox="0 0 24 24" fill="var(--coral)"><path d="M5,20h14v-2H5V20M19,9h-4V3H9v6H5l7,7L19,9Z"/></svg>
            </div>
            <div><div class="pipe-label">Download</div><div class="pipe-sub">2 serials</div></div>
            <svg class="pipe-check" width="14" height="14" viewBox="0 0 24 24" fill="var(--coral)"><path d="M9,20.42L2.79,14.21L5.62,11.38L9,14.77L18.88,4.88L21.71,7.71L9,20.42Z"/></svg>
            <div class="pipe-chevron"><svg width="8" height="8" viewBox="0 0 24 24" fill="var(--tx3)"><path d="M8.59,16.58L13.17,12L8.59,7.41L10,6L16,12L10,18Z"/></svg></div>
        </div>
        <div class="pipe-stage">
            <div class="pipe-icon" style="background:var(--amberbg)">
                <svg width="14" height="14" viewBox="0 0 24 24" fill="var(--amber)"><path d="M9.5,3A6.5,6.5 0 0,1 16,9.5c0,1.61-.59,3.09-1.56,4.23l.27.27H15.5l5,5-1.5,1.5-5-5v-.79l-.27-.27A6.52,6.52 0 0,1 9.5,16 6.5,6.5 0 0,1 3,9.5 6.5,6.5 0 0,1 9.5,3M9.5,5A4.5,4.5 0 0,0 5,9.5 4.5,4.5 0 0,0 9.5,14 4.5,4.5 0 0,0 14,9.5 4.5,4.5 0 0,0 9.5,5Z"/></svg>
            </div>
            <div><div class="pipe-label">Scrape</div><div class="pipe-sub">Gold + Forex</div></div>
            <svg class="pipe-check" width="14" height="14" viewBox="0 0 24 24" fill="var(--amber)"><path d="M9,20.42L2.79,14.21L5.62,11.38L9,14.77L18.88,4.88L21.71,7.71L9,20.42Z"/></svg>
            <div class="pipe-chevron"><svg width="8" height="8" viewBox="0 0 24 24" fill="var(--tx3)"><path d="M8.59,16.58L13.17,12L8.59,7.41L10,6L16,12L10,18Z"/></svg></div>
        </div>
        <div class="pipe-stage">
            <div class="pipe-icon" style="background:var(--bluebg)">
                <svg width="14" height="14" viewBox="0 0 24 24" fill="var(--blue)"><path d="M20,4H4A2,2 0 0,0 2,6V18A2,2 0 0,0 4,20H20A2,2 0 0,0 22,18V6A2,2 0 0,0 20,4M20,8L12,13 4,8V6L12,11 20,6V8Z"/></svg>
            </div>
            <div><div class="pipe-label">Deliver</div><div class="pipe-sub">5 sent</div></div>
            <svg class="pipe-check" width="14" height="14" viewBox="0 0 24 24" fill="var(--blue)"><path d="M9,20.42L2.79,14.21L5.62,11.38L9,14.77L18.88,4.88L21.71,7.71L9,20.42Z"/></svg>
            <div class="pipe-chevron"><svg width="8" height="8" viewBox="0 0 24 24" fill="var(--tx3)"><path d="M8.59,16.58L13.17,12L8.59,7.41L10,6L16,12L10,18Z"/></svg></div>
        </div>
        <div class="pipe-stage">
            <div class="pipe-icon" style="background:var(--tealbg)">
                <svg width="14" height="14" viewBox="0 0 24 24" fill="var(--teal)"><path d="M12,3C17.5,3 22,6.58 22,11C22,15.42 17.5,19 12,19C10.76,19 9.57,18.82 8.47,18.5C5.55,21 2,21 2,21C4.33,18.67 4.7,17.1 4.75,16.5C3.05,15.07 2,13.13 2,11C2,6.58 6.5,3 12,3Z"/></svg>
            </div>
            <div><div class="pipe-label">Listening</div><div class="pipe-sub">WhatsApp bot</div></div>
            <span class="live-dot" style="background:var(--teal);margin-left:auto"></span>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Metric cards
    m1, m2, m3, m4 = st.columns(4)
    with m1:
        st.markdown(f'<div class="mc"><div class="mc-label">Gold 24k</div><div class="mc-value">Rs.{fin.get("gold_24k","N/A")}</div><div class="mc-sub">per gram</div></div>', unsafe_allow_html=True)
    with m2:
        st.markdown(f'<div class="mc"><div class="mc-label">Gold 22k</div><div class="mc-value">Rs.{fin.get("gold_22k","N/A")}</div><div class="mc-sub">per gram</div></div>', unsafe_allow_html=True)
    with m3:
        st.markdown(f'<div class="mc"><div class="mc-label">CAD / INR</div><div class="mc-value">{fin.get("cad_to_inr","N/A")}</div><div class="mc-sub">exchange rate</div></div>', unsafe_allow_html=True)
    with m4:
        st.markdown(f'<div class="mc"><div class="mc-label">Deliveries</div><div class="mc-value">{ok_receipts}/{len(receipts)}</div><div class="mc-sub">all successful</div></div>', unsafe_allow_html=True)

    st.write("")

    # Media + Delivery cards
    c1, c2 = st.columns(2)
    with c1:
        vid_html = '<div class="crd"><div class="crd-title">Media downloads</div>'
        for i, v in enumerate(latest.get("video_results", [])):
            badge_cls = "badge-ok" if v["status"] == "success" else "badge-fail"
            border = 'border-top:0.5px solid var(--bd);' if i > 0 else ''
            vid_html += f'''<div style="display:flex;align-items:center;justify-content:space-between;gap:8px;padding:10px 0;{border}">
                <div><div style="font-size:14px;font-weight:500;color:var(--tx1)">{v["serial_name"]}</div>
                <div style="font-size:12px;font-family:var(--mono);color:var(--tx3)">{v.get("file_path","")}</div></div>
                <span class="badge {badge_cls}">{v["status"]}</span></div>'''
        vid_html += '</div>'
        st.markdown(vid_html, unsafe_allow_html=True)

    with c2:
        status = latest.get("delivery_status", "unknown")
        s_badge = "badge-ok" if status == "sent" else "badge-fail"
        rcpt_html = f'<div class="crd"><div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px"><span class="crd-title" style="margin:0">Delivery log</span><span class="badge {s_badge}">{status}</span></div>'
        for r in receipts:
            dot_color = "var(--ok)" if r.get("success") else "var(--fail)"
            rcpt_html += f'''<div class="rcpt">
                <span class="rcpt-dot" style="background:{dot_color}"></span>
                <span class="rcpt-cat">{r.get("category","")}</span>
                <span class="rcpt-phone">{r.get("phone","")}</span>
                <span class="rcpt-label">{r.get("label","")}</span></div>'''
        rcpt_html += '</div>'
        st.markdown(rcpt_html, unsafe_allow_html=True)

    # Agent status strip
    AGENTS = [
        {"name": "Master orchestrator", "dur": "3m 22s", "status": "idle", "color": "var(--purple)", "bg": "var(--purplebg)", "tc": "var(--purpletx)"},
        {"name": "Video downloader", "dur": "2m 55s", "status": "idle", "color": "var(--coral)", "bg": "var(--coralbg)", "tc": "var(--coraltx)"},
        {"name": "Financial scraper", "dur": "5s", "status": "idle", "color": "var(--amber)", "bg": "var(--amberbg)", "tc": "var(--ambertx)"},
        {"name": "Delivery agent", "dur": "17s", "status": "idle", "color": "var(--blue)", "bg": "var(--bluebg)", "tc": "var(--bluetx)"},
        {"name": "WhatsApp bot", "dur": "always-on", "status": "listening", "color": "var(--teal)", "bg": "var(--tealbg)", "tc": "var(--tealtx)", "live": True},
    ]

    strip_html = '<div style="display:flex;justify-content:space-between;align-items:center;margin-top:16px;margin-bottom:10px"><span style="font-size:14px;font-weight:500;color:var(--tx1)">Agent status</span></div>'
    strip_html += '<div style="display:flex;gap:8px;flex-wrap:wrap">'
    for a in AGENTS:
        live_dot = f'<span class="live-dot" style="background:{a["color"]}"></span>' if a.get("live") else ""
        strip_html += f'''<div style="display:flex;align-items:center;gap:8px;padding:8px 14px;background:var(--bg1);border:0.5px solid var(--bd);border-left:3px solid {a["color"]};flex:1 1 150px;min-width:0">
            <div style="min-width:0;flex:1"><div style="font-size:13px;font-weight:500;color:var(--tx1);overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{a["name"]}</div>
            <div style="font-size:11px;font-family:var(--mono);color:var(--tx3)">{a["dur"]}</div></div>
            <span class="badge" style="background:{a["bg"]};color:{a["tc"]}">{live_dot}{a["status"]}</span></div>'''
    strip_html += '</div>'
    st.markdown(strip_html, unsafe_allow_html=True)

    st.markdown(f'<div style="font-size:12px;color:var(--tx3);text-align:right;margin-top:12px">Data as of {fin.get("timestamp","N/A")}</div>', unsafe_allow_html=True)

    # Report history
    if has_report and len(reports) > 1:
        with st.expander("Report history"):
            for rp in reports[:10]:
                with open(rp) as f:
                    r = json.load(f)
                st.json(r)

# ===== TAB 2: AGENTS =====
with tab2:

    # Architecture SVG
    st.markdown("""
    <div style="background:var(--bg2);border-radius:var(--radius);padding:20px 8px;margin-bottom:20px;overflow:hidden">
    <svg viewBox="0 0 730 470" style="width:100%;height:auto;display:block">
        <defs><marker id="ah" markerWidth="7" markerHeight="5" refX="7" refY="2.5" orient="auto"><path d="M0,0 L7,2.5 L0,5" fill="var(--tx3)"/></marker></defs>

        <!-- Flow paths with animated dashes -->
        <path d="M445,125 C400,160 200,160 115,195" fill="none" stroke="var(--purple)" stroke-width="1.5" class="flow-line"/>
        <path d="M445,125 C430,160 370,170 335,195" fill="none" stroke="var(--purple)" stroke-width="1.5" class="flow-line"/>
        <path d="M445,125 C460,160 530,170 555,195" fill="none" stroke="var(--purple)" stroke-width="1.5" class="flow-line"/>
        <path d="M115,310 C115,340 200,360 310,375" fill="none" stroke="var(--coral)" stroke-width="1.5" class="flow-line"/>
        <path d="M335,310 Q335,345 310,375" fill="none" stroke="var(--amber)" stroke-width="1.5" class="flow-line"/>
        <path d="M555,310 C555,340 420,360 410,375" fill="none" stroke="var(--teal)" stroke-width="1.5" class="flow-line"/>
        <path d="M640,260 Q710,260 710,140 Q710,60 540,70" fill="none" stroke="var(--tx3)" stroke-width="1" stroke-dasharray="3,3" marker-end="url(#ah)"/>
        <text x="708" y="150" font-size="10" fill="var(--tx3)" text-anchor="end" font-family="var(--mono)">commands</text>

        <!-- Master orchestrator -->
        <rect x="350" y="40" width="190" height="108" rx="12" fill="var(--bg1)" stroke="var(--purple)" stroke-width="1"/>
        <rect x="351" y="41" width="188" height="3" rx="0" fill="var(--purple)"/>
        <circle cx="445" cy="72" r="16" fill="var(--purplebg)" stroke="var(--purple)" stroke-width="1"/>
        <circle cx="440" cy="69" r="2.5" fill="var(--purple)"/><circle cx="450" cy="69" r="2.5" fill="var(--purple)"/>
        <rect x="440" y="75" width="10" height="2" rx="1" fill="var(--purple)"/>
        <text x="445" y="105" text-anchor="middle" font-size="12" font-weight="500" fill="var(--tx1)">Master orchestrator</text>
        <text x="445" y="119" text-anchor="middle" font-size="10" fill="var(--tx2)">Schedules + delegates</text>
        <rect x="390" y="128" width="110" height="16" rx="8" fill="var(--purplebg)"/>
        <text x="445" y="139" text-anchor="middle" font-size="9" fill="var(--purpletx)" font-family="var(--mono)">04:00 UTC daily</text>

        <!-- Video downloader -->
        <rect x="30" y="195" width="170" height="115" rx="12" fill="var(--bg1)" stroke="var(--coral)" stroke-width="1"/>
        <rect x="31" y="196" width="168" height="3" rx="0" fill="var(--coral)"/>
        <circle cx="115" cy="227" r="16" fill="var(--coralbg)" stroke="var(--coral)" stroke-width="1"/>
        <circle cx="110" cy="224" r="2.5" fill="var(--coral)"/><circle cx="120" cy="224" r="2.5" fill="var(--coral)"/>
        <rect x="110" y="230" width="10" height="2" rx="1" fill="var(--coral)"/>
        <text x="115" y="260" text-anchor="middle" font-size="12" font-weight="500" fill="var(--tx1)">Video downloader</text>
        <text x="115" y="274" text-anchor="middle" font-size="10" fill="var(--tx2)">Selenium + FFmpeg</text>
        <rect x="45" y="282" width="65" height="14" rx="7" fill="var(--coralbg)"/>
        <text x="77" y="292" text-anchor="middle" font-size="8" fill="var(--coraltx)" font-family="var(--mono)">HLS .m3u8</text>
        <rect x="115" y="282" width="40" height="14" rx="7" fill="var(--coralbg)"/>
        <text x="135" y="292" text-anchor="middle" font-size="8" fill="var(--coraltx)" font-family="var(--mono)">.mp4</text>
        <rect x="160" y="282" width="30" height="14" rx="7" fill="var(--coralbg)"/>
        <text x="175" y="292" text-anchor="middle" font-size="8" fill="var(--coraltx)" font-family="var(--mono)">s1/2</text>

        <!-- Financial scraper -->
        <rect x="250" y="195" width="170" height="115" rx="12" fill="var(--bg1)" stroke="var(--amber)" stroke-width="1"/>
        <rect x="251" y="196" width="168" height="3" rx="0" fill="var(--amber)"/>
        <circle cx="335" cy="227" r="16" fill="var(--amberbg)" stroke="var(--amber)" stroke-width="1"/>
        <circle cx="330" cy="224" r="2.5" fill="var(--amber)"/><circle cx="340" cy="224" r="2.5" fill="var(--amber)"/>
        <rect x="330" y="230" width="10" height="2" rx="1" fill="var(--amber)"/>
        <text x="335" y="260" text-anchor="middle" font-size="12" font-weight="500" fill="var(--tx1)">Financial scraper</text>
        <text x="335" y="274" text-anchor="middle" font-size="10" fill="var(--tx2)">BeautifulSoup + lxml</text>
        <rect x="262" y="282" width="60" height="14" rx="7" fill="var(--amberbg)"/>
        <text x="292" y="292" text-anchor="middle" font-size="8" fill="var(--ambertx)" font-family="var(--mono)">Gold 22/24k</text>
        <rect x="327" y="282" width="55" height="14" rx="7" fill="var(--amberbg)"/>
        <text x="354" y="292" text-anchor="middle" font-size="8" fill="var(--ambertx)" font-family="var(--mono)">CAD/INR</text>

        <!-- WhatsApp bot -->
        <rect x="470" y="195" width="170" height="115" rx="12" fill="var(--bg1)" stroke="var(--teal)" stroke-width="1"/>
        <rect x="471" y="196" width="168" height="3" rx="0" fill="var(--teal)"/>
        <circle cx="555" cy="227" r="16" fill="var(--tealbg)" stroke="var(--teal)" stroke-width="1"/>
        <circle cx="550" cy="224" r="2.5" fill="var(--teal)"/><circle cx="560" cy="224" r="2.5" fill="var(--teal)"/>
        <rect x="550" y="230" width="10" height="2" rx="1" fill="var(--teal)"/>
        <circle cx="626" cy="209" r="4" fill="var(--teal)"><animate attributeName="opacity" values="1;0.3;1" dur="2s" repeatCount="indefinite"/></circle>
        <text x="555" y="260" text-anchor="middle" font-size="12" font-weight="500" fill="var(--tx1)">WhatsApp bot</text>
        <text x="555" y="274" text-anchor="middle" font-size="10" fill="var(--tx2)">Flask + Gemini NLP</text>
        <rect x="487" y="282" width="60" height="14" rx="7" fill="var(--tealbg)"/>
        <text x="517" y="292" text-anchor="middle" font-size="8" fill="var(--tealtx)" font-family="var(--mono)">Webhook</text>
        <rect x="553" y="282" width="70" height="14" rx="7" fill="var(--tealbg)"/>
        <text x="588" y="292" text-anchor="middle" font-size="8" fill="var(--tealtx)" font-family="var(--mono)">Intent parser</text>

        <!-- Delivery agent -->
        <rect x="210" y="370" width="200" height="95" rx="12" fill="var(--bg1)" stroke="var(--blue)" stroke-width="1"/>
        <rect x="211" y="371" width="198" height="3" rx="0" fill="var(--blue)"/>
        <circle cx="310" cy="397" r="14" fill="var(--bluebg)" stroke="var(--blue)" stroke-width="1"/>
        <circle cx="305" cy="394" r="2" fill="var(--blue)"/><circle cx="315" cy="394" r="2" fill="var(--blue)"/>
        <rect x="305" y="399" width="10" height="2" rx="1" fill="var(--blue)"/>
        <text x="310" y="425" text-anchor="middle" font-size="12" font-weight="500" fill="var(--tx1)">Delivery agent</text>
        <text x="310" y="438" text-anchor="middle" font-size="10" fill="var(--tx2)">Multi-target Twilio routing</text>
        <rect x="225" y="445" width="50" height="14" rx="7" fill="var(--bluebg)"/>
        <text x="250" y="455" text-anchor="middle" font-size="8" fill="var(--bluetx)" font-family="var(--mono)">Videos</text>
        <rect x="280" y="445" width="60" height="14" rx="7" fill="var(--bluebg)"/>
        <text x="310" y="455" text-anchor="middle" font-size="8" fill="var(--bluetx)" font-family="var(--mono)">Financial</text>
        <rect x="345" y="445" width="50" height="14" rx="7" fill="var(--bluebg)"/>
        <text x="370" y="455" text-anchor="middle" font-size="8" fill="var(--bluetx)" font-family="var(--mono)">Report</text>

        <!-- External sources -->
        <path d="M60,356 Q80,330 90,315" fill="none" stroke="var(--tx3)" stroke-width="1" stroke-dasharray="3,3" marker-end="url(#ah)"/>
        <rect x="10" y="356" width="80" height="32" rx="6" fill="var(--bg3)" stroke="var(--bd)" stroke-width="0.5"/>
        <text x="50" y="372" text-anchor="middle" font-size="10" font-weight="500" fill="var(--tx1)">Tamildhool</text>
        <text x="50" y="383" text-anchor="middle" font-size="8" fill="var(--tx3)" font-family="var(--mono)">.m3u8</text>

        <path d="M90,410 Q180,380 260,330" fill="none" stroke="var(--tx3)" stroke-width="1" stroke-dasharray="3,3" marker-end="url(#ah)"/>
        <rect x="10" y="400" width="80" height="32" rx="6" fill="var(--bg3)" stroke="var(--bd)" stroke-width="0.5"/>
        <text x="50" y="416" text-anchor="middle" font-size="10" font-weight="500" fill="var(--tx1)">AngelOne</text>
        <text x="50" y="427" text-anchor="middle" font-size="8" fill="var(--tx3)" font-family="var(--mono)">Gold rates</text>

        <path d="M90,450 Q200,430 280,340" fill="none" stroke="var(--tx3)" stroke-width="1" stroke-dasharray="3,3" marker-end="url(#ah)"/>
        <rect x="10" y="440" width="80" height="28" rx="6" fill="var(--bg3)" stroke="var(--bd)" stroke-width="0.5"/>
        <text x="50" y="458" text-anchor="middle" font-size="10" font-weight="500" fill="var(--tx1)">Remitly</text>

        <path d="M650,410 Q500,400 410,400" fill="none" stroke="var(--tx3)" stroke-width="1" stroke-dasharray="3,3" marker-end="url(#ah)"/>
        <rect x="640" y="400" width="80" height="32" rx="6" fill="var(--bg3)" stroke="var(--bd)" stroke-width="0.5"/>
        <text x="680" y="416" text-anchor="middle" font-size="10" font-weight="500" fill="var(--tx1)">WhatsApp</text>
        <text x="680" y="427" text-anchor="middle" font-size="8" fill="var(--tx3)" font-family="var(--mono)">User phone</text>
    </svg>
    </div>
    """, unsafe_allow_html=True)

    # Agent detail cards (bento)
    st.markdown('<div style="font-size:14px;font-weight:500;color:var(--tx1);margin-bottom:12px">Agent details</div>', unsafe_allow_html=True)

    AGENT_DETAILS = [
        {"name": "Master orchestrator", "role": "Coordinates all sub-agents, schedules daily runs, consolidates reports",
         "status": "idle", "last": "04:03:23", "dur": "3m 22s", "tasks": 11, "fail": 0, "color": "var(--purple)", "bg": "var(--purplebg)", "tc": "var(--purpletx)", "deps": []},
        {"name": "Video downloader", "role": "Selenium-Wire HLS interception + FFmpeg conversion to .mp4",
         "status": "idle", "last": "04:02:58", "dur": "2m 55s", "tasks": 2, "fail": 0, "color": "var(--coral)", "bg": "var(--coralbg)", "tc": "var(--coraltx)", "deps": ["Selenium-Wire", "FFmpeg", "Chrome"]},
        {"name": "Financial scraper", "role": "BeautifulSoup scraping of gold rates and CAD/INR forex",
         "status": "idle", "last": "04:03:04", "dur": "5s", "tasks": 3, "fail": 0, "color": "var(--amber)", "bg": "var(--amberbg)", "tc": "var(--ambertx)", "deps": ["BeautifulSoup", "Requests", "lxml"]},
        {"name": "Delivery agent", "role": "Multi-target WhatsApp routing via Twilio API",
         "status": "idle", "last": "04:03:22", "dur": "17s", "tasks": 5, "fail": 0, "color": "var(--blue)", "bg": "var(--bluebg)", "tc": "var(--bluetx)", "deps": ["Twilio", "PyWhatKit"]},
        {"name": "WhatsApp bot", "role": "Flask webhook + Gemini intent parser for conversational control",
         "status": "listening", "last": "09:33 PM", "dur": "always-on", "tasks": 3, "fail": 0, "color": "var(--teal)", "bg": "var(--tealbg)", "tc": "var(--tealtx)", "deps": ["Flask", "Gemini API", "Twilio"], "live": True},
    ]

    # First row: master spans 2 cols
    ac1, ac2 = st.columns([2, 1])
    for idx, col_ref in enumerate([ac1, ac2]):
        if idx >= len(AGENT_DETAILS):
            break
        a = AGENT_DETAILS[idx]
        pct = round(a["tasks"] / max(a["tasks"] + a["fail"], 1) * 100)
        live = '<span class="live-dot" style="background:' + a["color"] + '"></span>' if a.get("live") else ""
        deps = "".join(f'<span class="dep-pill">{d}</span>' for d in a["deps"])
        with col_ref:
            st.markdown(f'''<div class="agent-card">
                <div class="agent-header">
                    <div style="display:flex;align-items:center;gap:10px">
                        <div class="agent-icon" style="background:{a["bg"]}"><div style="width:8px;height:8px;border-radius:50%;background:{a["color"]}"></div></div>
                        <div><div style="font-size:14px;font-weight:500;color:var(--tx1)">{a["name"]}</div>
                        <div style="font-size:12px;color:var(--tx3)">{a["role"]}</div></div>
                    </div>
                    <span class="badge" style="background:{a["bg"]};color:{a["tc"]}">{live}{a["status"]}</span>
                </div>
                <div class="agent-stats">
                    <div class="agent-stat"><div class="agent-stat-label">Last run</div><div class="agent-stat-value">{a["last"]}</div></div>
                    <div class="agent-stat"><div class="agent-stat-label">Duration</div><div class="agent-stat-value">{a["dur"]}</div></div>
                    <div class="agent-stat"><div class="agent-stat-label">Success</div><div class="agent-stat-value">{pct}%</div></div>
                </div>
                <div style="display:flex;justify-content:space-between;margin-bottom:4px"><span style="font-size:11px;color:var(--tx3)">Task completion</span><span style="font-size:11px;font-family:var(--mono);color:var(--tx2)">{a["tasks"]}/{a["tasks"]+a["fail"]}</span></div>
                <div class="prog-track"><div class="prog-fill" style="width:{pct}%;background:{a["color"]}"></div></div>
                <div>{deps}</div>
            </div>''', unsafe_allow_html=True)

    # Remaining agents: 3 columns
    cols = st.columns(3)
    for idx, a in enumerate(AGENT_DETAILS[2:]):
        pct = round(a["tasks"] / max(a["tasks"] + a["fail"], 1) * 100)
        live = '<span class="live-dot" style="background:' + a["color"] + '"></span>' if a.get("live") else ""
        deps = "".join(f'<span class="dep-pill">{d}</span>' for d in a["deps"])
        with cols[idx % 3]:
            st.markdown(f'''<div class="agent-card">
                <div class="agent-header">
                    <div style="display:flex;align-items:center;gap:10px">
                        <div class="agent-icon" style="background:{a["bg"]}"><div style="width:8px;height:8px;border-radius:50%;background:{a["color"]}"></div></div>
                        <div><div style="font-size:14px;font-weight:500;color:var(--tx1)">{a["name"]}</div>
                        <div style="font-size:12px;color:var(--tx3)">{a["role"]}</div></div>
                    </div>
                    <span class="badge" style="background:{a["bg"]};color:{a["tc"]}">{live}{a["status"]}</span>
                </div>
                <div class="agent-stats">
                    <div class="agent-stat"><div class="agent-stat-label">Last run</div><div class="agent-stat-value">{a["last"]}</div></div>
                    <div class="agent-stat"><div class="agent-stat-label">Duration</div><div class="agent-stat-value">{a["dur"]}</div></div>
                    <div class="agent-stat"><div class="agent-stat-label">Success</div><div class="agent-stat-value">{pct}%</div></div>
                </div>
                <div style="display:flex;justify-content:space-between;margin-bottom:4px"><span style="font-size:11px;color:var(--tx3)">Task completion</span><span style="font-size:11px;font-family:var(--mono);color:var(--tx2)">{a["tasks"]}/{a["tasks"]+a["fail"]}</span></div>
                <div class="prog-track"><div class="prog-fill" style="width:{pct}%;background:{a["color"]}"></div></div>
                <div>{deps}</div>
            </div>''', unsafe_allow_html=True)

# ===== TAB 3: WHATSAPP BOT =====
with tab3:
    w1, w2 = st.columns(2)
    with w1:
        st.markdown('<div style="font-size:14px;font-weight:500;color:var(--tx1);margin-bottom:12px">WhatsApp conversation</div>', unsafe_allow_html=True)
        msgs = [
            {"from": "bot", "text": "DMFIA Daily Report - 10-04-2026\n\n[OK] Singapenne: success\n[OK] Annam: success\n\nGold 24k: Rs.14,947/gm\nCAD/INR: 60.50\n\nDelivery: sent (5/5)", "time": "8:05 PM"},
            {"from": "user", "text": "gold rates", "time": "9:12 PM"},
            {"from": "bot", "text": "Gold 24k: Rs.14,947/gm\nGold 22k: Rs.13,702/gm\nCAD/INR: 60.50\nAs of: 2026-04-10 20:00 EDT", "time": "9:12 PM"},
            {"from": "user", "text": "download singapenne", "time": "9:30 PM"},
            {"from": "bot", "text": "Got it! Working on: download_video\nThis may take a few minutes...", "time": "9:30 PM"},
            {"from": "user", "text": "status", "time": "9:40 PM"},
            {"from": "bot", "text": "Last Report: 10-04-2026\n[OK] Singapenne: success\n[OK] Annam: success\nGold 24k: 14,947\nDelivery: sent (5/5)", "time": "9:40 PM"},
        ]
        chat_html = '<div class="wa-wrap">'
        chat_html += '<div style="text-align:center;margin-bottom:12px"><span style="font-size:11px;color:var(--tx3);background:var(--bg3);padding:3px 12px;border-radius:8px">Today</span></div>'
        for m in msgs:
            cls = "wa-usr" if m["from"] == "user" else "wa-bot"
            chat_html += f'<div class="wa-bub {cls}">{m["text"]}<div class="wa-time">{m["time"]}</div></div>'
        chat_html += '</div>'
        st.markdown(chat_html, unsafe_allow_html=True)
        st.caption("Send commands on WhatsApp to control the agent")

    with w2:
        st.markdown('<div style="font-size:14px;font-weight:500;color:var(--tx1);margin-bottom:12px">Routing targets</div>', unsafe_allow_html=True)
        targets = config.get("whatsapp_targets", {})
        tgt_html = '<div class="crd">'
        for cat, tlist in targets.items():
            tgt_html += f'<div style="font-size:12px;color:var(--tx3);margin-bottom:6px;margin-top:10px;letter-spacing:0.04em">{cat}</div>'
            for t in tlist:
                tgt_html += f'<div class="tgt-row"><span class="tgt-phone">{t.get("phone","")}</span><span class="tgt-label">{t.get("label","")}</span></div>'
        tgt_html += '</div>'
        st.markdown(tgt_html, unsafe_allow_html=True)

        st.markdown('<div style="font-size:14px;font-weight:500;color:var(--tx1);margin:20px 0 12px">Commands reference</div>', unsafe_allow_html=True)
        cmds = [
            ("download singapenne", "Episode for today"),
            ("download annam for 08-04-2026", "Specific date"),
            ("gold rates", "Gold + forex data"),
            ("run all", "Full daily pipeline"),
            ("status", "Last report summary"),
            ("help", "List all commands"),
        ]
        cmd_html = '<div class="crd">'
        for i, (cmd, desc) in enumerate(cmds):
            border = 'border-top:0.5px solid var(--bd);' if i > 0 else ''
            cmd_html += f'<div style="display:flex;justify-content:space-between;align-items:center;padding:7px 0;{border}"><code style="font-size:12px;font-family:var(--mono)">{cmd}</code><span style="font-size:12px;color:var(--tx3)">{desc}</span></div>'
        cmd_html += '</div>'
        st.markdown(cmd_html, unsafe_allow_html=True)

# ===== TAB 4: ACTIVITY =====
with tab4:
    st.markdown('<div style="font-size:14px;font-weight:500;color:var(--tx1);margin-bottom:16px">Run timeline - 10-04-2026</div>', unsafe_allow_html=True)

    TIMELINE = [
        ("04:00:01", "Daily run triggered", "#888780"),
        ("04:00:03", "Singapenne download started", "#534AB7"),
        ("04:01:45", "Singapenne download complete (142 MB)", "#0F6E56"),
        ("04:01:46", "Annam download started", "#534AB7"),
        ("04:02:58", "Annam download complete (128 MB)", "#0F6E56"),
        ("04:03:02", "Gold rates scraped from AngelOne", "#BA7517"),
        ("04:03:04", "CAD/INR fetched from Remitly", "#BA7517"),
        ("04:03:08", "Videos sent to Sn, Mom", "#185FA5"),
        ("04:03:15", "Financial sent to Sn", "#185FA5"),
        ("04:03:22", "Full report sent to Sn, Mom", "#185FA5"),
        ("04:03:23", "Report saved. All tasks complete.", "#888780"),
    ]

    tl_html = '<div style="max-width:600px">'
    for i, (t, ev, color) in enumerate(TIMELINE):
        line = '' if i == len(TIMELINE) - 1 else '<div class="tl-line"></div>'
        tl_html += f'<div class="tl-row"><div style="position:relative"><div class="tl-dot" style="background:{color}"></div>{line}</div><div style="display:flex;justify-content:space-between;align-items:baseline;gap:8px;flex:1"><span class="tl-text">{ev}</span><span class="tl-time">{t}</span></div></div>'
    tl_html += '</div>'
    tl_html += '<div style="margin-top:8px;font-size:12px;color:var(--tx3)">Total duration: 3 min 22 sec</div>'

    legend = {"system": "#888780", "video": "#534AB7", "success": "#0F6E56", "finance": "#BA7517", "delivery": "#185FA5"}
    tl_html += '<div style="display:flex;gap:12px;margin-top:16px;flex-wrap:wrap">'
    for label, color in legend.items():
        tl_html += f'<span style="display:flex;align-items:center;gap:4px;font-size:12px;color:var(--tx2)"><span style="width:8px;height:8px;border-radius:50%;background:{color}"></span>{label}</span>'
    tl_html += '</div>'
    st.markdown(tl_html, unsafe_allow_html=True)

# ===== TAB 5: SETTINGS =====
with tab5:
    s1, s2 = st.columns(2)

    with s1:
        # Serials
        st.markdown('<div style="font-size:14px;font-weight:500;color:var(--tx1);margin-bottom:12px">Serials</div>', unsafe_allow_html=True)
        for i, s in enumerate(config.get("serials", [])):
            with st.expander(s["name"], expanded=False):
                config["serials"][i]["name"] = st.text_input(f"Name##s{i}", s["name"], key=f"sn{i}")
                config["serials"][i]["base_url"] = st.text_input(f"URL##s{i}", s["base_url"], key=f"su{i}")

        new_serial = st.text_input("Add serial name", key="new_s")
        new_url = st.text_input("Add serial URL", key="new_u")
        if st.button("Add Serial") and new_serial and new_url:
            config["serials"].append({"name": new_serial, "base_url": new_url})

        # Data sources with URLs
        st.markdown('<div style="font-size:14px;font-weight:500;color:var(--tx1);margin:24px 0 12px">Data sources</div>', unsafe_allow_html=True)
        sources = [
            ("IBJA", "Gold", "https://ibjarates.com/", "var(--amberbg)", "var(--ambertx)"),
            ("Angel One", "Gold", "https://www.angelone.in/gold-rates-today", "var(--amberbg)", "var(--ambertx)"),
            ("GoodReturns", "Gold", "https://www.goodreturns.in/gold/", "var(--amberbg)", "var(--ambertx)"),
            ("Remitly", "Forex", "https://www.remitly.com/ca/en/currency-converter/cad-to-inr-rate", "var(--purplebg)", "var(--purpletx)"),
        ]
        src_html = '<div class="crd">'
        for i, (name, cat, url, bg, tc) in enumerate(sources):
            border = 'border-top:0.5px solid var(--bd);' if i > 0 else ''
            src_html += f'''<div style="padding:10px 0;{border}">
                <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:4px">
                    <span style="font-size:13px;font-weight:500;color:var(--tx1)">{name}</span>
                    <span class="badge" style="background:{bg};color:{tc}">{cat}</span>
                </div>
                <a href="{url}" target="_blank" style="font-size:12px;font-family:var(--mono);color:var(--infotx);text-decoration:none;word-break:break-all;line-height:1.4">{url}</a>
            </div>'''
        src_html += '</div>'
        st.markdown(src_html, unsafe_allow_html=True)

    with s2:
        # System
        st.markdown('<div style="font-size:14px;font-weight:500;color:var(--tx1);margin-bottom:12px">System</div>', unsafe_allow_html=True)
        sys_html = '<div class="crd">'
        sys_html += '<div style="margin-bottom:14px"><div style="font-size:12px;color:var(--tx3);margin-bottom:6px">Schedule</div>'
        sys_html += f'<div style="font-size:16px;font-family:var(--mono);font-weight:500;color:var(--tx1)">{config.get("schedule_utc","04:00")} UTC daily</div></div>'
        sys_html += '<div style="border-top:0.5px solid var(--bd);padding-top:14px"><div style="font-size:12px;color:var(--tx3);margin-bottom:10px">Services</div>'
        for name, desc in [("Scheduler", "APScheduler cron"), ("WhatsApp bot", "Flask :5000"), ("Dashboard", "Streamlit :8501")]:
            sys_html += f'''<div style="display:flex;align-items:center;justify-content:space-between;padding:6px 0">
                <div style="display:flex;align-items:center;gap:8px"><span style="width:6px;height:6px;border-radius:50%;background:var(--ok)"></span><span style="font-size:13px;color:var(--tx1)">{name}</span></div>
                <span style="font-size:12px;font-family:var(--mono);color:var(--tx3)">{desc}</span></div>'''
        sys_html += '</div></div>'
        st.markdown(sys_html, unsafe_allow_html=True)

        # Environment
        st.markdown('<div style="font-size:14px;font-weight:500;color:var(--tx1);margin:20px 0 12px">Environment</div>', unsafe_allow_html=True)
        env_html = '<div class="crd">'
        for i, k in enumerate(["GEMINI_API_KEY", "TWILIO_ACCOUNT_SID", "TWILIO_AUTH_TOKEN", "HEADLESS"]):
            border = 'border-top:0.5px solid var(--bd);' if i > 0 else ''
            env_html += f'<div style="display:flex;align-items:center;justify-content:space-between;padding:6px 0;{border}"><code style="font-size:12px;font-family:var(--mono)">{k}</code><span class="badge badge-ok">set</span></div>'
        env_html += '</div>'
        st.markdown(env_html, unsafe_allow_html=True)

        # WhatsApp targets
        st.markdown('<div style="font-size:14px;font-weight:500;color:var(--tx1);margin:20px 0 12px">WhatsApp targets</div>', unsafe_allow_html=True)
        targets = config.get("whatsapp_targets", {})
        CATEGORIES = {
            "videos": ("Videos", "Downloaded .mp4 files"),
            "financial": ("Financial", "Gold & forex summaries"),
            "consolidated_report": ("Full Report", "Consolidated daily report"),
        }
        for cat_key, (cat_label, cat_desc) in CATEGORIES.items():
            st.markdown(f"**{cat_label}**")
            st.caption(cat_desc)
            cat_targets = targets.get(cat_key, [])
            for j, entry in enumerate(cat_targets):
                c1, c2, c3 = st.columns([3, 3, 1])
                with c1:
                    cat_targets[j]["phone"] = st.text_input(f"Phone###{cat_key}_{j}", entry.get("phone", ""), key=f"wp_{cat_key}_{j}", label_visibility="collapsed", placeholder="+1234567890")
                with c2:
                    cat_targets[j]["label"] = st.text_input(f"Label###{cat_key}_{j}", entry.get("label", ""), key=f"wl_{cat_key}_{j}", label_visibility="collapsed", placeholder="Label")
                with c3:
                    if st.button("X", key=f"wd_{cat_key}_{j}"):
                        cat_targets.pop(j)
                        st.rerun()

            ac1, ac2, ac3 = st.columns([3, 3, 1])
            with ac1:
                new_phone = st.text_input(f"Phone###{cat_key}", key=f"np_{cat_key}", placeholder="+1234567890", label_visibility="collapsed")
            with ac2:
                new_label = st.text_input(f"Label###{cat_key}", key=f"nl_{cat_key}", placeholder="Label", label_visibility="collapsed")
            with ac3:
                if st.button("+", key=f"na_{cat_key}") and new_phone:
                    cat_targets.append({"phone": new_phone, "label": new_label or new_phone})
            targets[cat_key] = cat_targets

        if st.button("Save Config", type="primary"):
            config["whatsapp_targets"] = targets
            config.pop("phone", None)
            with open(CONFIG_PATH, "w") as f:
                yaml.dump(config, f, default_flow_style=False)
            st.success("Config saved!")

    # Manual run
    st.divider()
    st.markdown('<div style="font-size:14px;font-weight:500;color:var(--tx1);margin-bottom:12px">Manual run</div>', unsafe_allow_html=True)
    rc1, rc2, rc3, rc4, rc5 = st.columns([2, 1, 1, 1, 1])
    with rc1:
        date_input = st.date_input("Date", datetime.now(), key="run_date")
    with rc2:
        run_video = st.checkbox("Videos", value=True)
    with rc3:
        run_finance = st.checkbox("Finance", value=True)
    with rc4:
        run_delivery = st.checkbox("WhatsApp", value=False)
    with rc5:
        st.write("")
        st.write("")
        if st.button("Run Now", type="primary"):
            date_str = date_input.strftime("%d-%m-%Y")
            with st.spinner(f"Running DMFIA for {date_str}..."):
                try:
                    orch = MasterOrchestrator()
                    if not run_video:
                        orch.video_agent.config["serials"] = []
                    if not run_delivery:
                        orch.delivery_agent.disable_all()
                    report = orch.run_daily(date_str)
                    st.success(f"Done! Delivery: {report.delivery_status}")
                    st.text(report.to_consolidated_text())
                except Exception as e:
                    st.error(f"Run failed: {e}")
