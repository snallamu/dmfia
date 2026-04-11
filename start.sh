#!/bin/sh
# Start scheduler in background
python scheduler.py &

# Start Streamlit in background on fixed internal port
streamlit run app.py --server.port=8501 --server.address=0.0.0.0 --server.headless=true &

# Start Flask WhatsApp bot on $PORT (Railway routes here for healthcheck)
# Falls back to 5000 for local dev
export BOT_PORT="${PORT:-5000}"
exec python whatsapp_bot.py
