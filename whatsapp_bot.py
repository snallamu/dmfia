"""
DMFIA WhatsApp Bot - Talk to the Master Agent via WhatsApp

Architecture:
  WhatsApp -> Twilio Sandbox -> Flask Webhook -> Gemini Intent Parser
  -> MasterOrchestrator sub-agents -> Reply via Twilio -> WhatsApp

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

# Dont call logging.basicConfig here - crewai_agents already did it.
# Just get a child logger.
logger = logging.getLogger("DMFIA.Bot")

app = Flask(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

TWILIO_SID = os.getenv("TWILIO_ACCOUNT_SID", "")
TWILIO_TOKEN = os.getenv("TWILIO_AUTH_TOKEN", "")
TWILIO_WHATSAPP_FROM = os.getenv("TWILIO_WHATSAPP_NUMBER", "whatsapp:+14155238886")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
VALIDATE_TWILIO_SIGNATURE = os.getenv("VALIDATE_TWILIO_SIGNATURE", "true").lower() == "true"

# ---------------------------------------------------------------------------
# Singleton orchestrator (avoid re-creating on every request)
# ---------------------------------------------------------------------------
_orchestrator = None
_orch_lock = threading.Lock()


def get_orchestrator():
    global _orchestrator
    if _orchestrator is None:
        with _orch_lock:
            if _orchestrator is None:
                from crewai_agents import MasterOrchestrator
                _orchestrator = MasterOrchestrator()
    return _orchestrator


# ---------------------------------------------------------------------------
# Twilio helpers
# ---------------------------------------------------------------------------

def validate_twilio_request(req):
    """Validate that the request actually came from Twilio."""
    if not VALIDATE_TWILIO_SIGNATURE:
        return True
    if not TWILIO_SID or not TWILIO_TOKEN:
        return True  # cant validate without creds, allow through
    try:
        from twilio.request_validator import RequestValidator
        validator = RequestValidator(TWILIO_TOKEN)
        url = req.url
        # Twilio uses X-Twilio-Signature header
        signature = req.headers.get("X-Twilio-Signature", "")
        params = req.form.to_dict()
        return validator.validate(url, params, signature)
    except Exception as e:
        logger.warning(f"Twilio validation error: {e}")
        return True  # fail open on import/setup errors


def send_whatsapp_reply(to: str, body: str):
    """Send a WhatsApp reply via Twilio REST API."""
    if not TWILIO_SID or not TWILIO_TOKEN:
        logger.error("Twilio credentials not set. Cannot reply.")
        return False
    try:
        from twilio.rest import Client
        client = Client(TWILIO_SID, TWILIO_TOKEN)
        message = client.messages.create(
            from_=TWILIO_WHATSAPP_FROM, to=to, body=body,
        )
        logger.info(f"Reply sent: SID={message.sid}")
        return True
    except Exception as e:
        logger.error(f"Twilio send failed: {e}")
        return False


def send_whatsapp_media(to: str, media_url: str, caption: str = ""):
    """Send media (image/chart) via Twilio WhatsApp."""
    if not TWILIO_SID or not TWILIO_TOKEN:
        logger.error("Twilio credentials not set.")
        return False
    try:
        from twilio.rest import Client
        client = Client(TWILIO_SID, TWILIO_TOKEN)
        kwargs = {"from_": TWILIO_WHATSAPP_FROM, "to": to, "media_url": [media_url]}
        if caption:
            kwargs["body"] = caption
        message = client.messages.create(**kwargs)
        logger.info(f"Media sent: SID={message.sid} URL={media_url[:60]}")
        return True
    except Exception as e:
        logger.error(f"Twilio media send failed: {e}")
        return False# ---------------------------------------------------------------------------
# Intent parsing
# ---------------------------------------------------------------------------

def parse_intent_with_gemini(message: str) -> dict:
    """Use Gemini API to parse user intent from natural language."""
    if not GEMINI_API_KEY:
        return parse_intent_keywords(message)
    try:
        import google.generativeai as genai
        genai.configure(api_key=GEMINI_API_KEY)
        model = genai.GenerativeModel("gemini-2.5-flash")

        # Sanitize user input to prevent prompt injection
        safe_msg = message.replace('"', "'").replace("\\", "")[:500]

        prompt = (
            "You are an intent parser for a media/finance bot. "
            "Parse the user message into a JSON object.\n\n"
            "Available intents:\n"
            "- download_video: Download a specific serial (params: serial_name, date DD-MM-YYYY)\n"
            "- download_all_videos: Download all serials (params: date DD-MM-YYYY)\n"
            "- get_finance: Get current gold rates only (no prediction)\n"
            "- gold_report: Full gold report = current rates + India vs Canada comparison + prediction + chart (params: period = weekly/monthly/yearly)\n"
            "- predict_gold: Predict gold prices only (params: period = weekly/monthly/yearly)\n"
            "- delivery_report: Show delivery status per recipient (no params)\n"
            "- run_all: Run everything (params: date DD-MM-YYYY)\n"
            "- status: Show last report (no params)\n"
            "- help: Show help (no params)\n"
            "- unknown: Cannot understand\n\n"
            "Known serials: Singapenne, Annam\n"
            'If no date specified, use "today".\n'
            'For predict_gold: default period is "weekly". User may say "monthly prediction" or "yearly gold forecast".\n'
            "Respond ONLY with a JSON object, no markdown.\n\n"
            'Example: "download singapenne" -> {"intent": "download_video", "serial_name": "Singapenne", "date": "today"}\n'
            'Example: "gold rate" -> {"intent": "get_finance"}\n'
            'Example: "gold report" -> {"intent": "gold_report", "period": "weekly"}\n'
            'Example: "gold report monthly" -> {"intent": "gold_report", "period": "monthly"}\n'
            'Example: "predict gold monthly" -> {"intent": "predict_gold", "period": "monthly"}\n'
            'Example: "delivery report" -> {"intent": "delivery_report"}\n\n'
            f'User message: "{safe_msg}"'
        )
        response = model.generate_content(prompt)
        text = response.text.strip()
        text = re.sub(r"```json\s*", "", text)
        text = re.sub(r"```\s*", "", text)
        parsed = json.loads(text)
        logger.info(f"Gemini intent: {parsed}")
        return parsed
    except Exception as e:
        logger.warning(f"Gemini failed: {e}. Falling back to keywords.")
        return parse_intent_keywords(message)


def parse_intent_keywords(message: str) -> dict:
    """Fallback keyword-based intent parser."""
    msg = message.lower().strip()
    date_match = re.search(r"(\d{2}-\d{2}-\d{4})", msg)
    date_str = date_match.group(1) if date_match else "today"

    if msg in ("help", "?", "commands", "menu"):
        return {"intent": "help"}
    if msg in ("status", "last report", "report status"):
        return {"intent": "status"}
    if any(w in msg for w in ["delivery report", "delivery status", "who received", "recipients"]):
        return {"intent": "delivery_report"}
    if any(w in msg for w in ["gold report", "gold comparison", "compare gold", "india vs canada gold"]):
        period = "weekly"
        if "month" in msg:
            period = "monthly"
        elif "year" in msg:
            period = "yearly"
        return {"intent": "gold_report", "period": period}
    if any(w in msg for w in ["predict", "prediction", "forecast", "gold predict"]):
        period = "weekly"
        if "month" in msg:
            period = "monthly"
        elif "year" in msg:
            period = "yearly"
        elif "week" in msg:
            period = "weekly"
        return {"intent": "predict_gold", "period": period}
    if any(w in msg for w in ["run all", "run everything", "full report", "daily run"]):
        return {"intent": "run_all", "date": date_str}
    if any(w in msg for w in ["gold", "finance", "forex", "cad", "inr", "exchange"]):
        return {"intent": "get_finance"}
    if any(w in msg for w in ["download all", "all videos", "all serials"]):
        return {"intent": "download_all_videos", "date": date_str}
    if "download" in msg or "video" in msg:
        for name in ["singapenne", "annam"]:
            if name in msg:
                return {"intent": "download_video", "serial_name": name.capitalize(), "date": date_str}
        return {"intent": "download_all_videos", "date": date_str}
    for name in ["singapenne", "annam"]:
        if name in msg:
            return {"intent": "download_video", "serial_name": name.capitalize(), "date": date_str}
    return {"intent": "unknown"}


# ---------------------------------------------------------------------------
# Command handlers
# ---------------------------------------------------------------------------

def resolve_date(date_str: str) -> str:
    if not date_str or date_str == "today":
        from crewai_agents import today_edt
        return today_edt()
    return date_str


def handle_help() -> str:
    return (
        "*DMFIA Bot Commands*\n\n"
        "- *gold report* - Full report: rates + India vs Canada + prediction + chart\n"
        "- *gold report monthly* - Same with monthly prediction\n"
        "- *gold rates* - Quick current rates only\n"
        "- *predict gold weekly* - AI prediction + chart only\n"
        "- *delivery report* - Who received what\n"
        "- *download singapenne* - Download today's episode\n"
        "- *download annam* - Download today's episode\n"
        "- *run all* - Full daily pipeline\n"
        "- *status* - Last report summary\n"
        "- *help* - This message"
    )


def handle_status() -> str:
    reports_dir = Path("downloads")
    report_files = sorted(reports_dir.glob("report_*.json"), reverse=True)
    if not report_files:
        return "No reports found yet. Send *run all* to generate one."
    with open(report_files[0]) as f:
        r = json.load(f)
    lines = [f"*Last Report: {r['date']}*", ""]
    for v in r.get("video_results", []):
        icon = "OK" if v["status"] == "success" else "FAIL"
        lines.append(f"[{icon}] {v['serial_name']}: {v['status']}")
    fin = r.get("financial", {})
    if fin:
        lines.append("")
        from crewai_agents import FinancialData
        lines.append(f"Gold 24k: {FinancialData._fmt_rate(fin.get('gold_24k'))}")
        lines.append(f"Gold 22k: {FinancialData._fmt_rate(fin.get('gold_22k'))}")
        lines.append(f"CAD/INR: {fin.get('cad_to_inr', 'N/A')}")
    lines.append("")
    lines.append(f"Delivery: {r.get('delivery_status', 'N/A')}")
    return "\n".join(lines)


def handle_download_video(serial_name: str, date_str: str) -> str:
    date_str = resolve_date(date_str)
    orch = get_orchestrator()
    serial_cfg = None
    for s in orch.config.get("serials", []):
        if s["name"].lower() == serial_name.lower():
            serial_cfg = s
            break
    if not serial_cfg:
        available = ", ".join(s["name"] for s in orch.config.get("serials", []))
        return f"Serial '{serial_name}' not found. Available: {available}"
    result = orch.video_agent.download_serial(serial_cfg, date_str)
    if result.status == "success":
        return f"*{result.serial_name}* ({result.date_str}) downloaded!\nFile: {result.file_path}"
    return f"*{result.serial_name}* ({result.date_str}) FAILED.\nError: {result.error}"


def handle_download_all(date_str: str) -> str:
    date_str = resolve_date(date_str)
    orch = get_orchestrator()
    results = orch.video_agent.run(date_str)
    lines = [f"*Video Downloads for {date_str}*", ""]
    for r in results:
        icon = "OK" if r.status == "success" else "FAIL"
        lines.append(f"[{icon}] {r.serial_name}: {r.status}")
        if r.error:
            lines.append(f"    Error: {r.error}")
    return "\n".join(lines)


def handle_finance() -> str:
    orch = get_orchestrator()
    return orch.finance_agent.run().to_text()


def handle_predict_gold(period: str = "weekly") -> str:
    """Generate AI-powered gold price prediction with chart."""
    orch = get_orchestrator()
    return orch.prediction_agent.predict(period)


def handle_gold_report(period: str = "weekly") -> dict:
    """Full gold report: rates + India vs Canada comparison + prediction + chart."""
    orch = get_orchestrator()
    return orch.gold_report(period)


def handle_delivery_report() -> str:
    """Show per-target delivery status."""
    from crewai_agents import generate_delivery_report
    return generate_delivery_report()


def handle_run_all(date_str: str) -> str:
    date_str = resolve_date(date_str)
    orch = get_orchestrator()
    report = orch.run_daily(date_str)
    return report.to_consolidated_text()


# ---------------------------------------------------------------------------
# Main router
# ---------------------------------------------------------------------------

def route_command(intent: dict) -> str:
    action = intent.get("intent", "unknown")
    try:
        if action == "help":
            return handle_help()
        elif action == "status":
            return handle_status()
        elif action == "get_finance":
            return handle_finance()
        elif action == "predict_gold":
            result = handle_predict_gold(intent.get("period", "weekly"))
            # result is a dict with 'text' and 'chart_path'
            if isinstance(result, dict):
                return result  # handled specially in webhook
            return result
        elif action == "gold_report":
            return handle_gold_report(intent.get("period", "weekly"))
        elif action == "delivery_report":
            return handle_delivery_report()
        elif action == "download_video":
            serial = intent.get("serial_name", "")
            if not serial:
                return "Which serial? Try: *download singapenne* or *download annam*"
            return handle_download_video(serial, intent.get("date", "today"))
        elif action == "download_all_videos":
            return handle_download_all(intent.get("date", "today"))
        elif action == "run_all":
            return handle_run_all(intent.get("date", "today"))
        else:
            return (
                "I didn't understand that. Here's what I can do:\n\n"
                "- *gold report* - Rates + comparison + prediction + chart\n"
                "- *gold rates* - Quick rates\n"
                "- *download singapenne*\n- *delivery report*\n"
                "- *run all*\n- *status*\n- *help*"
            )
    except Exception as e:
        logger.exception(f"Command failed: {e}")
        return f"Error: {str(e)[:200]}"


# ---------------------------------------------------------------------------
# Flask webhook
# ---------------------------------------------------------------------------

@app.route("/charts/<filename>", methods=["GET"])
def serve_chart(filename):
    """Serve generated chart images for Twilio media messages."""
    from flask import send_from_directory
    charts_dir = os.path.join(os.getcwd(), "charts")
    return send_from_directory(charts_dir, filename)


@app.route("/webhook", methods=["POST"])
def whatsapp_webhook():
    # Validate Twilio signature
    if not validate_twilio_request(flask_request):
        logger.warning("Invalid Twilio signature - rejecting request")
        return "Forbidden", 403

    sender = flask_request.form.get("From", "")
    body = flask_request.form.get("Body", "").strip()
    logger.info(f"WhatsApp from {sender}: {body}")

    if not body:
        reply = "Send *help* to see what I can do."
    else:
        quick_intents = {"help", "status", "unknown", "delivery_report"}
        intent = parse_intent_with_gemini(body)
        action = intent.get("intent", "unknown")

        if action in quick_intents:
            reply = route_command(intent)
        else:
            send_whatsapp_reply(sender, f"Got it! Working on: *{action}*\nThis may take a few minutes...")

            def bg():
                try:
                    result = route_command(intent)

                    # Handle prediction with chart (returns dict)
                    if isinstance(result, dict) and "text" in result:
                        text_msg = result["text"]
                        chart_path = result.get("chart_path")

                        # Send text first
                        send_whatsapp_reply(sender, text_msg)

                        # Send chart image if available
                        if chart_path and os.path.exists(chart_path):
                            # Build public URL for the chart
                            chart_filename = os.path.basename(chart_path)
                            # Get Railway public URL from env or request
                            base_url = os.getenv("RAILWAY_PUBLIC_URL", "")
                            if not base_url:
                                # Try to construct from Railway domain
                                base_url = os.getenv("RAILWAY_STATIC_URL", "")
                            if not base_url:
                                # Fallback: use the webhook URL domain
                                base_url = flask_request.host_url.rstrip("/")

                            chart_url = f"{base_url}/charts/{chart_filename}"
                            logger.info(f"Sending chart: {chart_url}")
                            send_whatsapp_media(sender, chart_url, "Gold Price Prediction Chart")
                    else:
                        send_whatsapp_reply(sender, str(result))

                except Exception as e:
                    logger.exception(f"Background task failed: {e}")
                    send_whatsapp_reply(sender, f"Task failed: {str(e)[:200]}")

            threading.Thread(target=bg, daemon=True).start()
            return '<Response></Response>', 200, {'Content-Type': 'text/xml'}

    from xml.sax.saxutils import escape
    twiml = f'<Response><Message>{escape(str(reply))}</Message></Response>'
    return twiml, 200, {'Content-Type': 'text/xml'}


@app.route("/health", methods=["GET"])
def health():
    return {"status": "ok", "service": "DMFIA WhatsApp Bot"}, 200


@app.route("/", methods=["GET"])
def index():
    return (
        "<h2>DMFIA WhatsApp Bot</h2>"
        "<p>Webhook: <code>POST /webhook</code></p>"
        "<p>Health: <code>GET /health</code></p>"
    ), 200


if __name__ == "__main__":
    port = int(os.getenv("BOT_PORT", 5000))
    logger.info(f"Starting DMFIA WhatsApp Bot on port {port}")
    if not TWILIO_SID or not TWILIO_TOKEN:
        logger.warning("TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN not set.")
    if not GEMINI_API_KEY:
        logger.warning("GEMINI_API_KEY not set. Using keyword parser.")
    app.run(host="0.0.0.0", port=port, debug=False)
