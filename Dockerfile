# ---- Étape 1 : base Node.js ----
FROM node:18-bullseye

# Installer Chromium et dépendances nécessaires à Puppeteer
RUN apt-get update && apt-get install -y --no-install-recommends \
  ca-certificates chromium \
  fonts-liberation fonts-noto-color-emoji \
  libasound2 libatk1.0-0 libatk-bridge2.0-0 libc6 libcairo2 libcups2 \
  libdbus-1-3 libdrm2 libexpat1 libfontconfig1 libgbm1 libglib2.0-0 \
  libgtk-3-0 libnspr4 libnss3 libpango-1.0-0 libpangocairo-1.0-0 \
  libstdc++6 libu2f-udev libvulkan1 libx11-6 libx11-xcb1 libxcb1 \
  libxcomposite1 libxcursor1 libxdamage1 libxext6 libxfixes3 libxi6 \
  libxkbcommon0 libxrandr2 libxrender1 libxshmfence1 libxss1 libxtst6 \
  lsb-release wget xdg-utils \
  && rm -rf /var/lib/apt/lists/* && \
  chromium --version

# Empêcher Puppeteer de télécharger son propre Chromium et définir le binaire système
ENV PUPPETEER_SKIP_CHROMIUM_DOWNLOAD=true \
    PUPPETEER_EXECUTABLE_PATH=/usr/bin/chromium \
    NODE_ENV=production \
    PORT=8080

# Créer le dossier de travail
WORKDIR /app

# Copier les fichiers de config et installer les dépendances en mode production
COPY package*.json ./
RUN npm install --omit=dev

# Copier le reste du code
COPY . .

EXPOSE 8080

# Lancer ton serveur Express
CMD ["node", "index.js"]
