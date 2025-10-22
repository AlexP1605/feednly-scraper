# ---- Étape 1 : base Node.js ----
FROM node:18-bullseye

# Installer Chromium et dépendances nécessaires à Puppeteer
RUN apt-get update && apt-get install -y \
  chromium \
  fonts-liberation libasound2 libatk1.0-0 libatk-bridge2.0-0 libc6 libcairo2 libcups2 \
  libdbus-1-3 libexpat1 libfontconfig1 libgbm1 libglib2.0-0 libgtk-3-0 libnspr4 \
  libnss3 libpango-1.0-0 libpangocairo-1.0-0 libstdc++6 libx11-6 libx11-xcb1 \
  libxcb1 libxcomposite1 libxcursor1 libxdamage1 libxext6 libxfixes3 libxi6 \
  libxrandr2 libxrender1 libxss1 libxtst6 lsb-release fonts-noto-color-emoji \
  && rm -rf /var/lib/apt/lists/*

# Créer le dossier de travail
WORKDIR /app

# Copier les fichiers de config et installer les dépendances
COPY package*.json ./
RUN npm install

# Copier le reste du code
COPY . .

# Variables d'environnement
ENV PORT=8080
ENV PUPPETEER_EXECUTABLE_PATH=/usr/bin/chromium
ENV NODE_ENV=production

# Lancer ton serveur Express
CMD ["node", "index.js"]
