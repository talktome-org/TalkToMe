#!/usr/bin/env bash
set -e

echo "🔧 Conductor setup (secrets + resources + python venv)"

# -------------------------
# Preconditions
# -------------------------
if [ ! -f ~/.config/talktome/backend.env ]; then
  echo "❌ Missing ~/.config/talktome/backend.env"
  exit 1
fi

if [ ! -f ~/.config/talktome/Secrets.plist ]; then
  echo "❌ Missing ~/.config/talktome/Secrets.plist"
  exit 1
fi

if [ ! -d ~/.config/talktome/Resources ]; then
  echo "❌ Missing ~/.config/talktome/Resources"
  exit 1
fi

# -------------------------
# Link secrets + resources
# -------------------------
if [ ! -e Backend/.env ]; then
  ln -s ~/.config/talktome/backend.env Backend/.env
  echo "✔ Linked Backend/.env"
fi

if [ ! -e TalkToMe/Secrets.plist ]; then
  ln -s ~/.config/talktome/Secrets.plist TalkToMe/Secrets.plist
  echo "✔ Linked TalkToMe/Secrets.plist"
fi

if [ ! -e TalkToMe/Resources ]; then
  ln -s ~/.config/talktome/Resources TalkToMe/Resources
  echo "✔ Linked TalkToMe/Resources"
fi

# -------------------------
# Python venv setup
# -------------------------
if [ ! -d Backend/venv ]; then
  echo "🐍 Creating Python virtual environment"
  python3 -m venv Backend/venv
fi

echo "📦 Installing Python dependencies"
source Backend/venv/bin/activate
pip install --upgrade pip
pip install -r Backend/requirements.txt


echo "✅ Workspace fully ready"
