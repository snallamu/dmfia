"""
DMFIA WhatsApp Bot - Talk to the Master Agent via WhatsApp

Architecture:
  WhatsApp -> Twilio Sandbox -> Flask Webhook -> Gemini Intent Parser
  -> MasterOrchestrator sub-agents -> Reply via Twilio -> WhatsApp

Supported commands (natural language via Gemini, or exact keywords):
  "download singapenne"           -> VideoDownloaderAgent for today
  "download annam for 08-04-2026" -> VideoDownloaderAgent for specific date
  "gold rates"                    -> FinancialScraperAgent
  "gold report"                   -> Full gold report + chart
  "forex rates"                   -> Current CAD/INR + transfer calculator
  "forex report"                  -> CAD/INR 7-day prediction + chart
  "forex report monthly"          -> CAD/INR 30-day prediction + chart
  "full report" / "run all"       -> MasterOrchestrator.run_daily()
  "status"                        -> Show last report summary
  "help"                          -> List available commands
"""

import os
import re
import json
import logging
import threading
from pathlib import Path
from flask import Flask, request as flask_request
from dotenv import load_dotenv

load_dotenv()

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
# Singleton orchestrator
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
    if not VALIDATE_TWILIO_SIGNATURE:
        return True
    if not TWILIO_SID or not TWILIO_TOKEN:
        return True
    try:
        from twilio.request_validator import RequestValidator
        validator = RequestValidator(TWILIO_TOKEN)
        signature = req.headers.get("X-Twilio-Signature", "")
        return validator.validate(req.url, req.form.to_dict(), signature)
    except Exception as e:
        logger.warning(f"Twilio validation error: {e}")
        return True


def send_whatsapp_reply(to: str, body: str) -> bool:
    if not TWILIO_SID or not TWILIO_TOKEN:
        logger.error("Twilio credentials not set.")
        return False
    try:
        from twilio.rest import Client
        msg = Client(TWILIO_SID, TWILIO_TOKEN).messages.create(
            from_=TWILIO_WHATSAPP_FROM, to=to, body=body)
        logger.info(f"Reply sent: SID={msg.sid}")
        return True
    except Exception as e:
        logger.error(f"Twilio send failed: {e}")
        return False


def send_whatsapp_media(to: str, media_url: str, caption: str = "") -> bool:
    if not TWILIO_SID or not TWILIO_TOKEN:
        return False
    try:
        from twilio.rest import Client
        kwargs = dict(from_=TWILIO_WHATSAPP_FROM, to=to, media_url=[media_url])
        if caption:
            kwargs["body"] = caption
        msg = Client(TWILIO_SID, TWILIO_TOKEN).messages.create(**kwargs)
        logger.info(f"Media sent: SID={msg.sid}")
        return True
    except Exception as e:
        logger.error(f"Twilio media send failed: {e}")
        return False


# ---------------------------------------------------------------------------
# Intent parsing
# ---------------------------------------------------------------------------

def parse_intent_with_gemini(message: str) -> dict:
    if not GEMINI_API_KEY:
        return parse_intent_keywords(message)
    try:
        import google.generativeai as genai
        genai.configure(api_key=GEMINI_API_KEY)
        model = genai.GenerativeModel("gemini-2.5-flash")
        safe_msg = message.replace('"', "'").replace("\\", "")[:500]
        prompt = (
            "You are an intent parser for a media/finance bot. "
            "Parse the user message into a JSON object.\n\n"
            "Available intents:\n"
            "- download_video: Download a specific serial (params: serial_name, date DD-MM-YYYY)\n"
            "- download_all_videos: Download all serials (params: date DD-MM-YYYY)\n"
            "- get_finance: Get current gold + forex rates (no prediction)\n"
            "- gold_report: Full gold report = rates + India vs Canada comparison + prediction + chart (params: period = weekly/monthly/yearly)\n"
            "- predict_gold: Predict gold prices only (params: period = weekly/monthly/yearly)\n"
            "- forex_rates: Current CAD/INR rate only + transfer calculator (no chart)\n"
            "- forex_report: CAD/INR prediction + chart (params: period = weekly/monthly)\n"
            "- delivery_report: Show delivery status per recipient\n"
            "- run_all: Run everything (params: date DD-MM-YYYY)\n"
            "- status: Show last report\n"
            "- help: Show help\n"
            "- unknown: Cannot understand\n\n"
            "Known serials: Singapenne, Annam\n"
            'If no date specified, use "today".\n'
            'forex_rates = quick current rate only. forex_report = AI prediction + chart.\n'
            '"good time to transfer" or "should I send money" -> forex_report.\n'
            "Respond ONLY with a JSON object, no markdown.\n\n"
            'Examples:\n'
            '"download singapenne" -> {"intent": "download_video", "serial_name": "Singapenne", "date": "today"}\n'
            '"gold report monthly" -> {"intent": "gold_report", "period": "monthly"}\n'
            '"forex rates" -> {"intent": "forex_rates"}\n'
            '"forex report" -> {"intent": "forex_report", "period": "weekly"}\n'
            '"forex monthly" -> {"intent": "forex_report", "period": "monthly"}\n'
            '"cad inr prediction" -> {"intent": "forex_report", "period": "weekly"}\n'
            '"good time to transfer money" -> {"intent": "forex_report", "period": "weekly"}\n'
            '"delivery report" -> {"intent": "delivery_report"}\n\n'
            f'User message: "{safe_msg}"'
        )
        response = model.generate_content(prompt)
        text = re.sub(r"```json\s*|```\s*", "", response.text.strip())
        parsed = json.loads(text)
        logger.info(f"Gemini intent: {parsed}")
        return parsed
    except Exception as e:
        logger.warning(f"Gemini failed: {e}. Falling back to keywords.")
        return parse_intent_keywords(message)


def parse_intent_keywords(message: str) -> dict:
    msg = message.lower().strip()
    date_match = re.search(r"(\d{2}-\d{2}-\d{4})", msg)
    date_str = date_match.group(1) if date_match else "today"

    if msg in ("help", "?", "commands", "menu"):
        return {"intent": "help"}
    if msg in ("status", "last report", "report status"):
        return {"intent": "status"}
    if any(w in msg for w in ["delivery report", "delivery status", "who received"]):
        return {"intent": "delivery_report"}

    # Forex — check before gold (both match "rate")
    if any(w in msg for w in ["forex report", "cad inr report", "transfer report",
                               "exchange report", "good time to transfer", "should i transfer",
                               "should i send", "remittance"]):
        period = "monthly" if "month" in msg else "weekly"
        return {"intent": "forex_report", "period": period}
    if any(w in msg for w in ["forex rate", "forex", "cad inr", "cad rate", "exchange rate",
                               "transfer rate"]):
        if any(w in msg for w in ["predict", "report", "monthly", "week", "forecast"]):
            period = "monthly" if "month" in msg else "weekly"
            return {"intent": "forex_report", "period": period}
        return {"intent": "forex_rates"}

    # Gold
    if any(w in msg for w in ["gold report", "gold comparison", "compare gold", "india vs canada"]):
        period = "monthly" if "month" in msg else "yearly" if "year" in msg else "weekly"
        return {"intent": "gold_report", "period": period}
    if any(w in msg for w in ["predict", "prediction", "forecast"]):
        period = "monthly" if "month" in msg else "yearly" if "year" in msg else "weekly"
        return {"intent": "predict_gold", "period": period}
    if any(w in msg for w in ["run all", "run everything", "full report", "daily run"]):
        return {"intent": "run_all", "date": date_str}
    if any(w in msg for w in ["gold", "finance", "rates"]):
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
        "🎬 *Video*\n"
        "- *download singapenne* — Today's episode\n"
        "- *download annam* — Today's episode\n\n"
        "💰 *Gold*\n"
        "- *gold rates* — Quick current rates\n"
        "- *gold report* — Rates + India vs Canada + prediction + chart\n"
        "- *gold report monthly* — Monthly prediction\n\n"
        "💱 *Forex (CAD → INR)*\n"
        "- *forex rates* — Current rate + transfer calc\n"
        "- *forex report* — 7-day AI prediction + chart\n"
        "- *forex report monthly* — 30-day prediction + chart\n\n"
        "📋 *Reports*\n"
        "- *delivery report* — Who received what\n"
        "- *run all* — Full daily pipeline\n"
        "- *status* — Last report summary\n"
        "- *help* — This message"
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
        icon = "✅" if v["status"] == "success" else "❌"
        lines.append(f"{icon} {v['serial_name']}: {v['status']}")
    fin = r.get("financial", {})
    if fin:
        lines.append("")
        from crewai_agents import FinancialData
        lines.append(f"Gold 24k: {FinancialData._fmt_rate(fin.get('gold_24k'))}")
        lines.append(f"Gold 22k: {FinancialData._fmt_rate(fin.get('gold_22k'))}")
        lines.append(f"CAD/INR: {fin.get('cad_to_inr', 'N/A')}")
    lines.append(f"\nDelivery: {r.get('delivery_status', 'N/A')}")
    return "\n".join(lines)


def handle_download_video(serial_name: str, date_str: str) -> str:
    date_str = resolve_date(date_str)
    orch = get_orchestrator()
    serial_cfg = next(
        (s for s in orch.config.get("serials", []) if s["name"].lower() == serial_name.lower()),
        None
    )
    if not serial_cfg:
        available = ", ".join(s["name"] for s in orch.config.get("serials", []))
        return f"Serial '{serial_name}' not found. Available: {available}"
    result = orch.video_agent.download_serial(serial_cfg, date_str)
    if result.status == "success":
        return f"✅ *{result.serial_name}* ({result.date_str}) downloaded!\nFile: {result.file_path}"
    return f"❌ *{result.serial_name}* ({result.date_str}) FAILED.\nError: {result.error}"


def handle_download_all(date_str: str) -> str:
    date_str = resolve_date(date_str)
    orch = get_orchestrator()
    results = orch.video_agent.run(date_str)
    lines = [f"*Video Downloads for {date_str}*", ""]
    for r in results:
        icon = "✅" if r.status == "success" else "❌"
        lines.append(f"{icon} {r.serial_name}: {r.status}")
        if r.error:
            lines.append(f"    Error: {r.error}")
    return "\n".join(lines)


def handle_finance() -> str:
    orch = get_orchestrator()
    return orch.finance_agent.run().to_text()


def handle_predict_gold(period: str = "weekly") -> dict:
    return get_orchestrator().prediction_agent.predict(period)


def handle_gold_report(period: str = "weekly") -> dict:
    return get_orchestrator().gold_report(period)


def handle_forex_rates() -> str:
    return get_orchestrator().forex_rates()


def handle_forex_report(period: str = "weekly") -> dict:
    return get_orchestrator().forex_report(period)


def handle_delivery_report() -> str:
    from crewai_agents import generate_delivery_report
    return generate_delivery_report()


def handle_run_all(date_str: str) -> str:
    date_str = resolve_date(date_str)
    report = get_orchestrator().run_daily(date_str)
    return report.to_consolidated_text()


# ---------------------------------------------------------------------------
# Main router
# ---------------------------------------------------------------------------

def route_command(intent: dict):
    action = intent.get("intent", "unknown")
    try:
        if action == "help":
            return handle_help()
        elif action == "status":
            return handle_status()
        elif action == "get_finance":
            return handle_finance()
        elif action == "predict_gold":
            return handle_predict_gold(intent.get("period", "weekly"))
        elif action == "gold_report":
            return handle_gold_report(intent.get("period", "weekly"))
        elif action == "forex_rates":
            return handle_forex_rates()
        elif action == "forex_report":
            return handle_forex_report(intent.get("period", "weekly"))
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
                "I didn't understand that. Send *help* for commands.\n\n"
                "Quick examples:\n"
                "- *gold report*\n- *forex rates*\n"
                "- *forex report monthly*\n- *download singapenne*"
            )
    except Exception as e:
        logger.exception(f"Command failed: {e}")
        return f"Error: {str(e)[:200]}"


# ---------------------------------------------------------------------------
# Chart sender helper
# ---------------------------------------------------------------------------

def _send_result_with_chart(sender: str, result):
    if isinstance(result, dict) and "text" in result:
        send_whatsapp_reply(sender, result["text"])
        chart_path = result.get("chart_path")
        if chart_path and os.path.exists(chart_path):
            chart_filename = os.path.basename(chart_path)
            base_url = (os.getenv("RAILWAY_PUBLIC_URL")
                        or os.getenv("RAILWAY_STATIC_URL")
                        or flask_request.host_url.rstrip("/"))
            chart_url = f"{base_url}/charts/{chart_filename}"
            logger.info(f"Sending chart: {chart_url}")
            send_whatsapp_media(sender, chart_url, "📊 Prediction Chart")
    else:
        send_whatsapp_reply(sender, str(result))


# ---------------------------------------------------------------------------
# Flask routes
# ---------------------------------------------------------------------------

@app.route("/charts/<filename>", methods=["GET"])
def serve_chart(filename):
    from flask import send_from_directory
    return send_from_directory(os.path.join(os.getcwd(), "charts"), filename)


@app.route("/webhook", methods=["POST"])
def whatsapp_webhook():
    if not validate_twilio_request(flask_request):
        logger.warning("Invalid Twilio signature — rejecting")
        return "Forbidden", 403

    sender = flask_request.form.get("From", "")
    body = flask_request.form.get("Body", "").strip()
    logger.info(f"WhatsApp from {sender}: {body}")

    if not body:
        reply = "Send *help* to see what I can do."
    else:
        intent = parse_intent_with_gemini(body)
        action = intent.get("intent", "unknown")
        quick_intents = {"help", "status", "unknown", "delivery_report"}

        if action in quick_intents:
            reply = route_command(intent)
        else:
            send_whatsapp_reply(sender, f"Got it! Working on: *{action}*\nThis may take a few minutes...")

            def bg():
                try:
                    result = route_command(intent)
                    _send_result_with_chart(sender, result)
                except Exception as e:
                    logger.exception(f"Background task failed: {e}")
                    send_whatsapp_reply(sender, f"Task failed: {str(e)[:200]}")

            threading.Thread(target=bg, daemon=True).start()
            return '<Response></Response>', 200, {'Content-Type': 'text/xml'}

    from xml.sax.saxutils import escape
    return (
        f'<Response><Message>{escape(str(reply))}</Message></Response>',
        200,
        {'Content-Type': 'text/xml'}
    )


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
