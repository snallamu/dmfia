"""
DMFIA WhatsApp Bot - Talk to the Master Agent via WhatsApp

Architecture:
  WhatsApp -> Twilio Sandbox (free) -> Flask Webhook -> Gemini Intent Parser
  -> MasterOrchestrator sub-agents -> Reply via Twilio -> WhatsApp

Setup (one-time, $0):
  1. Create free Twilio account at twilio.com/try-twilio
  2. Go to Console > Messaging > Try it out > Send a WhatsApp message
  3. Join sandbox: send "join <two-words>" to the Twilio sandbox number
  4. Set webhook URL to: https://<your-railway-url>/webhook
  5. Add TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN to env vars

Supported commands (natural language via Gemini, or exact keywords):
  "download singapenne"          -> VideoDownloaderAgent for today
  "download annam for 08-04-2026"-> VideoDownloaderAgent for specific date
  "gold rates" / "finance"       -> FinancialScraperAgent
  "full report" / "run all"      -> MasterOrchestrator.run_daily()
  "status"                       -> Show last report summary
  "help"                         -> List available commands
"""

import os
import re
import json
import logging
import threading
from datetime import datetime
from pathlib import Path
from flask import Flask, request as flask_request
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler("dmfia.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("DMFIA.Bot")

app = Flask(__name__)

# ---------------------------------------------------------------------------
# Twilio helpers
# ---------------------------------------------------------------------------

TWILIO_SID = os.getenv("TWILIO_ACCOUNT_SID", "")
TWILIO_TOKEN = os.getenv("TWILIO_AUTH_TOKEN", "")
TWILIO_WHATSAPP_FROM = os.getenv("TWILIO_WHATSAPP_NUMBER", "whatsapp:+14155238886")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")


def send_whatsapp_reply(to: str, body: str):
    """Send a WhatsApp reply via Twilio REST API."""
    if not TWILIO_SID or not TWILIO_TOKEN:
        logger.error("Twilio credentials not set. Cannot reply.")
        return False
    try:
        from twilio.rest import Client
        client = Client(TWILIO_SID, TWILIO_TOKEN)
        message = client.messages.create(
            from_=TWILIO_WHATSAPP_FROM,
            to=to,
            body=body,
        )
        logger.info(f"Reply sent to {to}: SID={message.sid}")
        return True
    except Exception as e:
        logger.error(f"Twilio send failed: {e}")
        return False


def send_whatsapp_media(to: str, media_url: str, caption: str = ""):
    """Send a media file via Twilio (file must be publicly accessible URL)."""
    if not TWILIO_SID or not TWILIO_TOKEN:
        return False
    try:
        from twilio.rest import Client
        client = Client(TWILIO_SID, TWILIO_TOKEN)
        message = client.messages.create(
            from_=TWILIO_WHATSAPP_FROM,
            to=to,
            body=caption,
            media_url=[media_url],
        )
        logger.info(f"Media sent to {to}: SID={message.sid}")
        return True
    except Exception as e:
        logger.error(f"Twilio media send failed: {e}")
        return False


# ---------------------------------------------------------------------------
# Intent parsing with Gemini
# ---------------------------------------------------------------------------

INTENT_SCHEMA = {
    "download_video": {
        "description": "Download a specific serial episode",
        "params": ["serial_name", "date"],
    },
    "download_all_videos": {
        "description": "Download all configured serials",
        "params": ["date"],
    },
    "get_finance": {
        "description": "Get gold rates and forex data",
        "params": [],
    },
    "run_all": {
        "description": "Run the full daily pipeline (videos + finance + delivery)",
        "params": ["date"],
    },
    "status": {
        "description": "Show the last report status",
        "params": [],
    },
    "help": {
        "description": "Show available commands",
        "params": [],
    },
    "unknown": {
        "description": "Could not understand the request",
        "params": [],
    },
}


def parse_intent_with_gemini(message: str) -> dict:
    """Use Gemini API to parse user intent from natural language."""
    if not GEMINI_API_KEY:
        # Fallback to keyword matching if no Gemini key
        return parse_intent_keywords(message)

    try:
        import google.generativeai as genai
        genai.configure(api_key=GEMINI_API_KEY)
        model = genai.GenerativeModel("gemini-2.5-flash")

        prompt = f"""You are an intent parser for a media/finance bot. Parse the user message into a JSON object.

Available intents:
- download_video: Download a specific serial (params: serial_name, date in DD-MM-YYYY)
- download_all_videos: Download all serials (params: date in DD-MM-YYYY)
- get_finance: Get gold rates and CAD/INR forex (no params)
- run_all: Run everything - videos + finance + send report (params: date in DD-MM-YYYY)
- status: Show last report (no params)
- help: Show help (no params)
- unknown: Cannot understand

Known serials: Singapenne, Annam

If no date specified, use "today".
Respond ONLY with a JSON object, no markdown, no backticks.

Example: "download singapenne" -> {{"intent": "download_video", "serial_name": "Singapenne", "date": "today"}}
Example: "gold rate" -> {{"intent": "get_finance"}}
Example: "run all for 08-04-2026" -> {{"intent": "run_all", "date": "08-04-2026"}}

User message: "{message}"
"""
        response = model.generate_content(prompt)
        text = response.text.strip()
        # Clean any markdown fences
        text = re.sub(r"```json\s*", "", text)
        text = re.sub(r"```\s*", "", text)
        parsed = json.loads(text)
        logger.info(f"Gemini parsed intent: {parsed}")
        return parsed

    except Exception as e:
        logger.warning(f"Gemini intent parsing failed: {e}. Falling back to keywords.")
        return parse_intent_keywords(message)


def parse_intent_keywords(message: str) -> dict:
    """Fallback keyword-based intent parser (no AI needed)."""
    msg = message.lower().strip()

    # Extract date if present (DD-MM-YYYY)
    date_match = re.search(r"(\d{2}-\d{2}-\d{4})", msg)
    date_str = date_match.group(1) if date_match else "today"

    if msg in ("help", "?", "commands", "menu"):
        return {"intent": "help"}

    if msg in ("status", "last report", "report status"):
        return {"intent": "status"}

    if any(w in msg for w in ["run all", "run everything", "full report", "daily run"]):
        return {"intent": "run_all", "date": date_str}

    if any(w in msg for w in ["gold", "finance", "forex", "cad", "inr", "exchange"]):
        return {"intent": "get_finance"}

    if any(w in msg for w in ["download all", "all videos", "all serials"]):
        return {"intent": "download_all_videos", "date": date_str}

    if "download" in msg or "video" in msg:
        # Try to find serial name
        for name in ["singapenne", "annam"]:
            if name in msg:
                return {
                    "intent": "download_video",
                    "serial_name": name.capitalize(),
                    "date": date_str,
                }
        # If serial name mentioned without download keyword
        return {"intent": "download_all_videos", "date": date_str}

    for name in ["singapenne", "annam"]:
        if name in msg:
            return {
                "intent": "download_video",
                "serial_name": name.capitalize(),
                "date": date_str,
            }

    return {"intent": "unknown"}


# ---------------------------------------------------------------------------
# Command handlers
# ---------------------------------------------------------------------------

def resolve_date(date_str: str) -> str:
    """Convert 'today' to actual DD-MM-YYYY."""
    if not date_str or date_str == "today":
        return datetime.now().strftime("%d-%m-%Y")
    return date_str


def handle_help() -> str:
    return (
        "*DMFIA Bot - Available Commands*\n\n"
        "You can type naturally or use these keywords:\n\n"
        "*Videos:*\n"
        "  - download singapenne\n"
        "  - download annam for 08-04-2026\n"
        "  - download all videos\n\n"
        "*Finance:*\n"
        "  - gold rates\n"
        "  - cad to inr\n"
        "  - get finance\n\n"
        "*Full Pipeline:*\n"
        "  - run all\n"
        "  - full report\n"
        "  - daily run for 09-04-2026\n\n"
        "*Status:*\n"
        "  - status\n"
        "  - last report\n\n"
        "*Other:*\n"
        "  - help"
    )


def handle_status() -> str:
    downloads = Path("downloads")
    reports = sorted(downloads.glob("report_*.json"), reverse=True) if downloads.exists() else []
    if not reports:
        return "No reports found yet. Send *run all* to trigger the first run."

    with open(reports[0]) as f:
        r = json.load(f)

    lines = [f"*Last Report: {r['date']}*", ""]

    for v in r.get("video_results", []):
        icon = "OK" if v["status"] == "success" else "FAIL"
        lines.append(f"[{icon}] {v['serial_name']}: {v['status']}")

    fin = r.get("financial", {})
    if fin:
        lines.append("")
        lines.append(f"Gold 22k: {fin.get('gold_22k', 'N/A')}")
        lines.append(f"Gold 24k: {fin.get('gold_24k', 'N/A')}")
        lines.append(f"CAD/INR: {fin.get('cad_to_inr', 'N/A')}")

    lines.append("")
    lines.append(f"Delivery: {r.get('delivery_status', 'N/A')}")

    receipts = r.get("delivery_receipts", [])
    if receipts:
        ok = sum(1 for x in receipts if x.get("success"))
        lines.append(f"Deliveries: {ok}/{len(receipts)} successful")

    return "\n".join(lines)


def handle_download_video(serial_name: str, date_str: str) -> str:
    """Download a single serial and return status."""
    from crewai_agents import MasterOrchestrator, load_config
    date_str = resolve_date(date_str)
    config = load_config()

    # Find the serial config
    serial_cfg = None
    for s in config.get("serials", []):
        if s["name"].lower() == serial_name.lower():
            serial_cfg = s
            break

    if not serial_cfg:
        available = ", ".join(s["name"] for s in config.get("serials", []))
        return f"Serial '{serial_name}' not found. Available: {available}"

    orch = MasterOrchestrator()
    result = orch.video_agent.download_serial(serial_cfg, date_str)

    if result.status == "success":
        return f"*{result.serial_name}* ({result.date_str}) downloaded successfully!\nFile: {result.file_path}"
    else:
        return f"*{result.serial_name}* ({result.date_str}) download FAILED.\nError: {result.error}"


def handle_download_all(date_str: str) -> str:
    """Download all configured serials."""
    from crewai_agents import MasterOrchestrator
    date_str = resolve_date(date_str)

    orch = MasterOrchestrator()
    results = orch.video_agent.run(date_str)

    lines = [f"*Video Downloads for {date_str}*", ""]
    for r in results:
        icon = "OK" if r.status == "success" else "FAIL"
        lines.append(f"[{icon}] {r.serial_name}: {r.status}")
        if r.error:
            lines.append(f"    Error: {r.error}")
        if r.file_path:
            lines.append(f"    File: {r.file_path}")
    return "\n".join(lines)


def handle_finance() -> str:
    """Scrape and return financial data."""
    from crewai_agents import MasterOrchestrator

    orch = MasterOrchestrator()
    fin = orch.finance_agent.run()

    lines = [
        "*Financial Update*",
        "",
        f"Gold 22k: Rs.{fin.gold_22k or 'N/A'}/gm",
        f"Gold 24k: Rs.{fin.gold_24k or 'N/A'}/gm",
        f"CAD/INR:  {fin.cad_to_inr or 'N/A'}",
        f"As of:    {fin.timestamp or 'N/A'}",
    ]
    if fin.errors:
        lines.append(f"\nWarnings: {'; '.join(fin.errors)}")
    return "\n".join(lines)


def handle_run_all(date_str: str) -> str:
    """Run the full daily pipeline."""
    from crewai_agents import MasterOrchestrator
    date_str = resolve_date(date_str)

    orch = MasterOrchestrator()
    report = orch.run_daily(date_str)
    return report.to_consolidated_text()


# ---------------------------------------------------------------------------
# Main router
# ---------------------------------------------------------------------------

def route_command(intent: dict) -> str:
    """Route parsed intent to the correct handler."""
    action = intent.get("intent", "unknown")

    try:
        if action == "help":
            return handle_help()

        elif action == "status":
            return handle_status()

        elif action == "get_finance":
            return handle_finance()

        elif action == "download_video":
            serial = intent.get("serial_name", "")
            date = intent.get("date", "today")
            if not serial:
                return "Which serial? Try: *download singapenne* or *download annam*"
            return handle_download_video(serial, date)

        elif action == "download_all_videos":
            date = intent.get("date", "today")
            return handle_download_all(date)

        elif action == "run_all":
            date = intent.get("date", "today")
            return handle_run_all(date)

        elif action == "unknown":
            return (
                "I didn't understand that. Here's what I can do:\n\n"
                "- *download singapenne*\n"
                "- *gold rates*\n"
                "- *run all*\n"
                "- *status*\n"
                "- *help*"
            )
        else:
            return f"Unknown intent: {action}. Send *help* for available commands."

    except Exception as e:
        logger.exception(f"Command execution failed: {e}")
        return f"Error executing command: {str(e)[:200]}"


# ---------------------------------------------------------------------------
# Flask webhook endpoints
# ---------------------------------------------------------------------------

@app.route("/webhook", methods=["POST"])
def whatsapp_webhook():
    """
    Twilio sends incoming WhatsApp messages here as POST form data.
    Fields: From, To, Body, NumMedia, MediaUrl0, etc.
    """
    sender = flask_request.form.get("From", "")      # e.g. "whatsapp:+16473386458"
    body = flask_request.form.get("Body", "").strip()
    num_media = int(flask_request.form.get("NumMedia", 0))

    logger.info(f"Incoming WhatsApp from {sender}: {body}")

    if not body:
        reply = "Send *help* to see what I can do."
    else:
        # Send immediate acknowledgment for long-running tasks
        quick_intents = {"help", "status", "unknown"}
        intent = parse_intent_with_gemini(body)
        action = intent.get("intent", "unknown")

        if action in quick_intents:
            # Fast response, reply inline
            reply = route_command(intent)
        else:
            # Long-running task: ack first, then process in background
            send_whatsapp_reply(sender, f"Got it! Working on: *{action}*\nThis may take a few minutes...")

            def background_task():
                result = route_command(intent)
                send_whatsapp_reply(sender, result)

            thread = threading.Thread(target=background_task, daemon=True)
            thread.start()

            # Return empty TwiML (no immediate reply since we acked above)
            return '<Response></Response>', 200, {'Content-Type': 'text/xml'}

    # Return TwiML response for quick replies
    from xml.sax.saxutils import escape
    twiml = f'<Response><Message>{escape(reply)}</Message></Response>'
    return twiml, 200, {'Content-Type': 'text/xml'}


@app.route("/health", methods=["GET"])
def health():
    """Health check endpoint."""
    return {"status": "ok", "service": "DMFIA WhatsApp Bot"}, 200


@app.route("/", methods=["GET"])
def index():
    """Landing page."""
    return (
        "<h2>DMFIA WhatsApp Bot</h2>"
        "<p>Send a WhatsApp message to interact with the Master Agent.</p>"
        "<p>Webhook: <code>POST /webhook</code></p>"
        "<p>Health: <code>GET /health</code></p>"
    ), 200


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    port = int(os.getenv("BOT_PORT", 5000))
    logger.info(f"Starting DMFIA WhatsApp Bot on port {port}")
    logger.info("Webhook URL: POST /webhook")

    if not TWILIO_SID or not TWILIO_TOKEN:
        logger.warning(
            "TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN not set. "
            "Bot will start but cannot send replies. "
            "Set these env vars and restart."
        )

    if not GEMINI_API_KEY:
        logger.warning(
            "GEMINI_API_KEY not set. Using keyword-based intent parsing. "
            "Set this env var for natural language understanding."
        )

    app.run(host="0.0.0.0", port=port, debug=False)
