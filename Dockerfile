FROM node:22-bullseye

# Install Ghostscript (and a couple of useful fonts)
RUN apt-get update \
  && apt-get install -y --no-install-recommends ghostscript fonts-dejavu-core \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install deps first (better caching)
COPY package*.json ./
RUN npm install --omit=dev

# Copy code
COPY . .

# Start worker
CMD ["npm", "run", "worker"]
