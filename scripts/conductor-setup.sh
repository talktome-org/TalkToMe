#!/usr/bin/env bash
set -e

# Keep repository operations signed (commits/merges/tags) for auditability.
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
# Link secrets (symlink OK)
# -------------------------
if [ ! -e Backend/.env ]; then
  ln -s ~/.config/talktome/backend.env Backend/.env
  echo "✔ Linked Backend/.env"
fi

if [ ! -e TalkToMe/Secrets.plist ]; then
  ln -s ~/.config/talktome/Secrets.plist TalkToMe/Secrets.plist
  echo "✔ Linked TalkToMe/Secrets.plist"
fi

# -------------------------
# Copy Resources (NO SYMLINKS)
# -------------------------
if [ -L TalkToMe/Resources ]; then
  echo "⚠️ Removing old symlinked Resources"
  rm TalkToMe/Resources
fi

if [ ! -d TalkToMe/Resources ]; then
  echo "📂 Copying Resources into workspace"
  cp -R ~/.config/talktome/Resources TalkToMe/
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
