#!/usr/bin/env bash
# Local/conductor-style setup with configurable venv and env paths.
# Override paths via env vars or edit the defaults below:
#   BACKEND_ENV_SOURCE  path to your .env (backend env)
#   VENV_DIR            path to your Python venv
#   CONFIG_DIR          path to talktome config (Secrets.plist, Resources); empty to skip
set -e

# ---------------------------------------------------------------------------
SCRIPT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$SCRIPT_ROOT"

# ---------------------------------------------------------------------------
# Custom paths (change these to match your machine)
# ---------------------------------------------------------------------------
# Where your backend env file lives.
# If unset, we'll auto-discover from common local locations.
BACKEND_ENV_SOURCE="${BACKEND_ENV_SOURCE:-}"

# Where your Python venv lives (default: Backend/venv)
VENV_DIR="${VENV_DIR:-$SCRIPT_ROOT/Backend/venv}"

# Optional: config dir for Secrets.plist and Resources (empty = skip; set to ~/.config/talktome if you use it)
CONFIG_DIR="${CONFIG_DIR:-}"

# Auto-discover backend env when not explicitly provided.
if [ -z "$BACKEND_ENV_SOURCE" ]; then
  for candidate in \
    "$SCRIPT_ROOT/.env" \
    "$SCRIPT_ROOT/Backend/.env" \
    "$HOME/.config/talktome/backend.env" \
    "$HOME/myapps/TalkToMe project/TalkToMe-1/Backend/.env"
  do
    if [ -f "$candidate" ]; then
      BACKEND_ENV_SOURCE="$candidate"
      break
    fi
  done

  if [ -z "$BACKEND_ENV_SOURCE" ] && [ -d "$HOME/myapps" ]; then
    DISCOVERED_ENV="$(find "$HOME/myapps" -maxdepth 6 -type f -path "*/TalkToMe*/Backend/.env" -print -quit 2>/dev/null || true)"
    if [ -n "$DISCOVERED_ENV" ]; then
      BACKEND_ENV_SOURCE="$DISCOVERED_ENV"
    fi
  fi
fi

echo "🔧 Local setup (secrets + resources + python venv)"
echo "   BACKEND_ENV_SOURCE=$BACKEND_ENV_SOURCE"
echo "   VENV_DIR=$VENV_DIR"
echo "   CONFIG_DIR=${CONFIG_DIR:-(skipped)}"

# -------------------------
# Preconditions
# -------------------------
if [ -z "$BACKEND_ENV_SOURCE" ] || [ ! -f "$BACKEND_ENV_SOURCE" ]; then
  echo "❌ Missing backend env."
  echo "   Checked: $SCRIPT_ROOT/.env, $SCRIPT_ROOT/Backend/.env, ~/.config/talktome/backend.env, ~/myapps/*/Backend/.env"
  echo "   Set BACKEND_ENV_SOURCE=/absolute/path/to/backend.env and rerun."
  exit 1
fi

if [ -n "$CONFIG_DIR" ] && [ ! -f "$CONFIG_DIR/Secrets.plist" ]; then
  echo "❌ Missing $CONFIG_DIR/Secrets.plist"
  exit 1
fi

if [ -n "$CONFIG_DIR" ] && [ ! -d "$CONFIG_DIR/Resources" ]; then
  echo "❌ Missing $CONFIG_DIR/Resources"
  exit 1
fi

# -------------------------
# Link backend env (symlink OK)
# -------------------------
if [ ! -e Backend/.env ]; then
  ln -s "$BACKEND_ENV_SOURCE" Backend/.env
  echo "✔ Linked Backend/.env -> $BACKEND_ENV_SOURCE"
fi

# -------------------------
# Link secrets (symlink OK)
# -------------------------
if [ -n "$CONFIG_DIR" ]; then
  if [ ! -e TalkToMe/Secrets.plist ]; then
    ln -s "$CONFIG_DIR/Secrets.plist" TalkToMe/Secrets.plist
    echo "✔ Linked TalkToMe/Secrets.plist"
  fi
fi

# -------------------------
# Copy Resources (NO SYMLINKS)
# -------------------------
if [ -n "$CONFIG_DIR" ]; then
  if [ -L TalkToMe/Resources ]; then
    echo "⚠️ Removing old symlinked Resources"
    rm TalkToMe/Resources
  fi

  if [ ! -d TalkToMe/Resources ]; then
    echo "📂 Copying Resources into workspace"
    cp -R "$CONFIG_DIR/Resources" TalkToMe/
  fi
fi

# -------------------------
# Python venv setup
# -------------------------
if [ ! -d "$VENV_DIR" ]; then
  echo "🐍 Creating Python virtual environment at $VENV_DIR"
  python3 -m venv "$VENV_DIR"
fi

echo "📦 Installing Python dependencies"
# shellcheck source=/dev/null
source "$VENV_DIR/bin/activate"
pip install --upgrade pip
pip install -r Backend/requirements.txt

echo "✅ Workspace ready (venv: $VENV_DIR, env: $BACKEND_ENV_SOURCE)"
