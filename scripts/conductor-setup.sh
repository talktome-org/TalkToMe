#!/usr/bin/env bash
set -e

echo "🔧 Conductor setup (real secrets)"

# Fail early if your real secrets store doesn't exist
if [ ! -f ~/.config/talktome/backend.env ]; then
  echo "❌ Missing ~/.config/talktome/backend.env"
  exit 1
fi

if [ ! -f ~/.config/talktome/Secrets.plist ]; then
  echo "❌ Missing ~/.config/talktome/Secrets.plist"
  exit 1
fi

# Link Backend/.env
if [ ! -f Backend/.env ]; then
  ln -s ~/.config/talktome/backend.env Backend/.env
  echo "✔ Linked Backend/.env"
fi

# Link TalkToMe/Secrets.plist
if [ ! -f TalkToMe/Secrets.plist ]; then
  ln -s ~/.config/talktome/Secrets.plist TalkToMe/Secrets.plist
  echo "✔ Linked TalkToMe/Secrets.plist"
fi

echo "✅ Done"

