#!/usr/bin/env bash
set -e

echo "🔧 Conductor workspace setup"

# Backend .env
if [ ! -f Backend/.env ]; then
  cp Backend/env.example Backend/.env
  echo "✔ Created Backend/.env"
fi

# iOS Secrets.plist
if [ ! -f TalkToMe/Secrets.plist ]; then
  cp TalkToMe/Secrets.example.plist TalkToMe/Secrets.plist
  echo "✔ Created TalkToMe/Secrets.plist"
fi

echo "✅ Workspace setup done"
