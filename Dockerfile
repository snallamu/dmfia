FROM python:3.12-slim

# System deps: Chrome, FFmpeg, fonts
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget gnupg2 unzip curl ffmpeg fonts-liberation \
    libnss3 libatk-bridge2.0-0 libgtk-3-0 libgbm1 libasound2 \
    libxshmfence1 libx11-xcb1 xdg-utils \
    && wget -q -O /tmp/chrome.deb https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb \
    && apt-get install -y /tmp/chrome.deb \
    && rm /tmp/chrome.deb \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Chromedriver
RUN CHROME_VER=$(google-chrome --version | grep -oP '\d+\.\d+\.\d+') \
    && wget -q "https://storage.googleapis.com/chrome-for-testing-public/${CHROME_VER}.0/linux64/chromedriver-linux64.zip" -O /tmp/cd.zip \
    || wget -q "https://edgedl.me.gvt1.com/edgedl/chrome/chrome-for-testing/${CHROME_VER}.0/linux64/chromedriver-linux64.zip" -O /tmp/cd.zip \
    || true
RUN if [ -f /tmp/cd.zip ]; then \
        unzip -o /tmp/cd.zip -d /tmp/cd && \
        mv /tmp/cd/chromedriver-linux64/chromedriver /usr/local/bin/ && \
        chmod +x /usr/local/bin/chromedriver && \
        rm -rf /tmp/cd /tmp/cd.zip; \
    fi

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p downloads charts

ENV HEADLESS=true
ENV DISPLAY=:99

EXPOSE 8501 5000

RUN chmod +x start.sh

CMD ["./start.sh"]
