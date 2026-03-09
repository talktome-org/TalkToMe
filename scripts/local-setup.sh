#!/usr/bin/env bash
# Local/conductor-style setup with configurable venv and env paths.
# Override paths via env vars or edit the defaults below:
#   BACKEND_ENV_SOURCE  path to your .env (backend env)
#   SECRETS_PLIST_SOURCE path to your real Secrets.plist (app secrets)
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

# Where your app secrets plist lives.
# If unset, we'll auto-discover from common local locations.
SECRETS_PLIST_SOURCE="${SECRETS_PLIST_SOURCE:-}"

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

# Auto-discover app secrets plist when not explicitly provided.
if [ -z "$SECRETS_PLIST_SOURCE" ]; then
  if [ -n "$CONFIG_DIR" ] && [ -f "$CONFIG_DIR/Secrets.plist" ]; then
    SECRETS_PLIST_SOURCE="$CONFIG_DIR/Secrets.plist"
  fi

  if [ -z "$SECRETS_PLIST_SOURCE" ] && [ -n "$BACKEND_ENV_SOURCE" ]; then
    BACKEND_ENV_DIR="$(cd "$(dirname "$BACKEND_ENV_SOURCE")" && pwd)"
    if [ -f "$BACKEND_ENV_DIR/Secrets.plist" ]; then
      SECRETS_PLIST_SOURCE="$BACKEND_ENV_DIR/Secrets.plist"
    fi
  fi

  if [ -z "$SECRETS_PLIST_SOURCE" ]; then
    for candidate in \
      "$HOME/.config/talktome/Secrets.plist" \
      "$HOME/myapps/TalkToMe project/TalkToMe-1/TalkToMe/Secrets.plist"
    do
      if [ -f "$candidate" ]; then
        SECRETS_PLIST_SOURCE="$candidate"
        break
      fi
    done
  fi

  if [ -z "$SECRETS_PLIST_SOURCE" ] && [ -d "$HOME/myapps" ]; then
    DISCOVERED_SECRETS="$(find "$HOME/myapps" -maxdepth 7 -type f -path "*/TalkToMe*/TalkToMe/Secrets.plist" -print -quit 2>/dev/null || true)"
    if [ -n "$DISCOVERED_SECRETS" ]; then
      SECRETS_PLIST_SOURCE="$DISCOVERED_SECRETS"
    fi
  fi
fi

echo "🔧 Local setup (secrets + resources + python venv)"
echo "   BACKEND_ENV_SOURCE=$BACKEND_ENV_SOURCE"
echo "   SECRETS_PLIST_SOURCE=${SECRETS_PLIST_SOURCE:-(not found)}"
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

if [ -n "$SECRETS_PLIST_SOURCE" ] && [ ! -f "$SECRETS_PLIST_SOURCE" ]; then
  echo "❌ SECRETS_PLIST_SOURCE points to a missing file: $SECRETS_PLIST_SOURCE"
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
# Ensure app secrets
# -------------------------
SECRETS_TARGET="BoBo/Secrets.plist"
SECRETS_PLACEHOLDER_PATTERN='YOUR_PROJECT_REF|sb_publishable_\.\.\.|https://example\.com'

# Clean up broken symlink so we can safely recreate the target.
if [ -L "$SECRETS_TARGET" ] && [ ! -e "$SECRETS_TARGET" ]; then
  rm "$SECRETS_TARGET"
fi

if [ -e "$SECRETS_TARGET" ]; then
  if grep -Eq "$SECRETS_PLACEHOLDER_PATTERN" "$SECRETS_TARGET"; then
    if [ -n "$SECRETS_PLIST_SOURCE" ] && [ -f "$SECRETS_PLIST_SOURCE" ]; then
      rm -f "$SECRETS_TARGET"
      ln -s "$SECRETS_PLIST_SOURCE" "$SECRETS_TARGET"
      echo "✔ Replaced placeholder BoBo/Secrets.plist -> $SECRETS_PLIST_SOURCE"
    else
      echo "⚠️  BoBo/Secrets.plist contains placeholder values and no real secrets source was found"
    fi
  fi
else
  if [ -n "$SECRETS_PLIST_SOURCE" ] && [ -f "$SECRETS_PLIST_SOURCE" ]; then
    ln -s "$SECRETS_PLIST_SOURCE" "$SECRETS_TARGET"
    echo "✔ Linked BoBo/Secrets.plist -> $SECRETS_PLIST_SOURCE"
  elif [ -f BoBo/Secrets.example.plist ]; then
    cp BoBo/Secrets.example.plist "$SECRETS_TARGET"
    echo "⚠️  Created BoBo/Secrets.plist from Secrets.example.plist (real secrets not found)"
  else
    echo "❌ Missing BoBo/Secrets.plist and BoBo/Secrets.example.plist"
    exit 1
  fi
fi

# -------------------------
# Copy Resources (NO SYMLINKS)
# -------------------------
if [ -n "$CONFIG_DIR" ]; then
  if [ -L BoBo/Resources ]; then
    echo "⚠️ Removing old symlinked Resources"
    rm BoBo/Resources
  fi

  if [ ! -d BoBo/Resources ]; then
    echo "📂 Copying Resources into workspace"
    cp -R "$CONFIG_DIR/Resources" BoBo/
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

echo "✅ Workspace ready (venv: $VENV_DIR, env: $BACKEND_ENV_SOURCE, secrets: ${SECRETS_PLIST_SOURCE:-BoBo/Secrets.example.plist})"
