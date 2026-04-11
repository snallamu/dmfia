# DMFIA - Daily Media & Financial Intelligence Agent

Multi-agent system you control via WhatsApp. Downloads Tamil serial episodes, scrapes gold/forex data, and sends consolidated reports.

## Architecture

```
WhatsApp Message
    |
    v
Twilio Sandbox (free) --> Flask Webhook (/webhook)
    |
    v
Gemini Intent Parser (natural language -> action)
    |
    v
MasterOrchestrator
  ├── VideoDownloaderAgent   --> Selenium-Wire + FFmpeg --> .mp4
  ├── FinancialScraperAgent  --> BeautifulSoup --> Gold/Forex
  └── DeliveryAgent          --> Twilio WhatsApp API --> Reply
    |
    v
WhatsApp Reply (result sent back to you)
```

## WhatsApp Commands

Talk naturally or use keywords:

| What you type | What happens |
|---|---|
| `download singapenne` | Downloads today's Singapenne episode |
| `download annam for 08-04-2026` | Downloads specific date |
| `download all videos` | Downloads all configured serials |
| `gold rates` | Scrapes current gold 22k/24k prices |
| `cad to inr` | Gets current CAD/INR exchange rate |
| `get finance` | Gets all financial data |
| `run all` | Full pipeline: videos + finance + delivery |
| `status` | Shows last report summary |
| `help` | Lists all commands |

## Setup (Free, $0)

### 1. Twilio WhatsApp Sandbox

1. Sign up free at [twilio.com/try-twilio](https://www.twilio.com/try-twilio)
2. Go to **Console > Messaging > Try it out > Send a WhatsApp message**
3. Note the sandbox number (usually `+14155238886`) and join code
4. From your phone, send `join <two-words>` to the sandbox number on WhatsApp
5. Copy your **Account SID** and **Auth Token** from the Twilio Console dashboard

### 2. Gemini API Key

1. Go to [aistudio.google.com/app/apikey](https://aistudio.google.com/app/apikey)
2. Create a new API key (free tier)

### 3. Local Development

```powershell
# Clone and setup
cd D:\AI_Agents\dmfia
copy .env.example .env
# Edit .env with your Twilio + Gemini credentials

# Install
pip install -r requirements.txt

# Start the WhatsApp bot
python whatsapp_bot.py

# In another terminal, start the dashboard
streamlit run app.py

# In another terminal, start the scheduler
python scheduler.py
```

### 4. Expose Webhook (for local dev)

Twilio needs a public URL. Use ngrok (free):

```powershell
# Install ngrok: https://ngrok.com/download
ngrok http 5000
```

Copy the `https://xxxx.ngrok.io` URL and set it in Twilio:
- Go to Twilio Console > Messaging > Settings > WhatsApp Sandbox
- Set **When a message comes in** to: `https://xxxx.ngrok.io/webhook`
- Method: POST

Now send a WhatsApp message to the sandbox number!

### 5. Deploy to Railway

```powershell
railway login
railway init
railway variables set TWILIO_ACCOUNT_SID=ACxxxxxxxx
railway variables set TWILIO_AUTH_TOKEN=your_token
railway variables set TWILIO_WHATSAPP_NUMBER=whatsapp:+14155238886
railway variables set GEMINI_API_KEY=your_gemini_key
railway variables set HEADLESS=true
railway up
```

Then set the Twilio webhook to your Railway URL:
`https://<your-app>.up.railway.app/webhook`

## Files

| File | Purpose |
|---|---|
| `whatsapp_bot.py` | Flask webhook + Gemini intent parser + agent router |
| `crewai_agents.py` | All agents + orchestrator |
| `app.py` | Streamlit dashboard |
| `scheduler.py` | APScheduler cron (04:00 UTC) |
| `config.yaml` | Serials, sites, WhatsApp targets |
| `Dockerfile` | Container with Chrome + FFmpeg + all services |
| `railway.toml` | Railway deploy config |

## Per-Category WhatsApp Routing

Videos, financial data, and reports can each go to different phone numbers. Edit `config.yaml`:

```yaml
whatsapp_targets:
  videos:
    - phone: "+16473386458"
      label: "Me"
    - phone: "+19055551234"
      label: "Mom"
  financial:
    - phone: "+16473386458"
      label: "Me"
  consolidated_report:
    - phone: "+16473386458"
      label: "Me"
```

## Adding a New Agent

Create a class, wire it into `MasterOrchestrator.run_daily()`, and add a new intent + handler in `whatsapp_bot.py`.
