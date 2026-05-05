#!/usr/bin/env bash
set -e

# Keep repository operations signed (commits/merges/tags) for auditability.
echo "🔧 Conductor setup (secrets + python venv)"

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

# -------------------------
# Link secrets (symlink OK)
# -------------------------
if [ ! -e Backend/.env ]; then
  ln -s ~/.config/talktome/backend.env Backend/.env
  echo "✔ Linked Backend/.env"
fi

if [ ! -e .env ]; then
  ln -s ~/.config/talktome/backend.env .env
  echo "✔ Linked .env"
fi

if [ ! -e BoBo/Secrets.plist ]; then
  ln -s ~/.config/talktome/Secrets.plist BoBo/Secrets.plist
  echo "✔ Linked BoBo/Secrets.plist"
fi

# -------------------------
# Python venv setup (ROOT)
# -------------------------
# Keep setup idempotent so reruns are safe in fresh or partially prepared workspaces.
if [ ! -d venv ]; then
  echo "🐍 Creating Python virtual environment (root)"
  python3 -m venv venv
fi

echo "📦 Installing Python dependencies"
source venv/bin/activate
pip install --upgrade pip
pip install -r Backend/requirements.txt

echo "✅ Workspace fully ready"
