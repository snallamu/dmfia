#!/bin/sh
# Start scheduler in background
python scheduler.py &

# Start Streamlit in background on fixed internal port
streamlit run app.py --server.port=8501 --server.address=0.0.0.0 --server.headless=true &

# Start Flask WhatsApp bot via gunicorn (production-grade)
# Falls back to Flask dev server if gunicorn not available
export BOT_PORT="${PORT:-5000}"

if command -v gunicorn > /dev/null 2>&1; then
    exec gunicorn --bind "0.0.0.0:${BOT_PORT}" --workers 2 --timeout 120 "whatsapp_bot:app"
else
    exec python whatsapp_bot.py
fi
