#!/usr/bin/env bash
set -e

echo "🔧 Conductor setup (real secrets + resources)"

# Fail early if your local store doesn't exist
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

# Link Backend/.env
if [ ! -e Backend/.env ]; then
  ln -s ~/.config/talktome/backend.env Backend/.env
  echo "✔ Linked Backend/.env"
fi

# Link TalkToMe/Secrets.plist
if [ ! -e TalkToMe/Secrets.plist ]; then
  ln -s ~/.config/talktome/Secrets.plist TalkToMe/Secrets.plist
  echo "✔ Linked TalkToMe/Secrets.plist"
fi

# Link Resources directory
if [ ! -e TalkToMe/Resources ]; then
  ln -s ~/.config/talktome/Resources TalkToMe/Resources
  echo "✔ Linked TalkToMe/Resources"
fi

echo "✅ Done"
