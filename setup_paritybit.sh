#!/usr/bin/env bash
set -euo pipefail

# -----------------------
# Simple bootstrap script
# -----------------------
# Usage:
# 1) Edit variables below if desired (GIT_REPO, PROJECT_DIR)
# 2) Run as root:
#    sudo ./setup_paritybit.sh
#
# The script will:
# - install tor, python3, git, pip, venv
# - create a non-root user 'crawler'
# - create project dir /opt/paritybit-crawler (or clone GIT_REPO)
# - create python venv and install pip deps
# - write a minimal /etc/tor/torrc (if tor package installed)
# - create systemd units for crawler and scooper, enable+start them
# - do a small verification at the end
# -----------------------

# === CONFIG (edit if you want) ===
GIT_REPO=""   # optional: "https://github.com/your/repo.git" (leave empty if you will copy files manually)
PROJECT_DIR="/opt/paritybit-crawler"
CRAWLER_USER="crawler"
CRAWLER_INTERVAL_MINUTES=60
PYTHON_REQUIREMENTS="requests[socks] pyyaml beautifulsoup4 lxml pysocks stem rapidfuzz pandas"
TORRC_PATH="/etc/tor/torrc"
# ================================

echo "Starting bootstrap: project_dir=$PROJECT_DIR user=$CRAWLER_USER"

if [ "$(id -u)" -ne 0 ]; then
  echo "This script must be run as root (sudo). Exiting."
  exit 1
fi

# Detect package manager
PKG_MANAGER=""
if command -v apt >/dev/null 2>&1; then
  PKG_MANAGER="apt"
elif command -v dnf >/dev/null 2>&1; then
  PKG_MANAGER="dnf"
elif command -v yum >/dev/null 2>&1; then
  PKG_MANAGER="yum"
else
  echo "Unsupported distro (no apt/dnf/yum). Install dependencies manually and re-run. Exiting."
  exit 1
fi
echo "Using package manager: $PKG_MANAGER"

# Update & install system packages
if [ "$PKG_MANAGER" = "apt" ]; then
  apt update
  apt install -y tor python3 python3-venv python3-pip git
else
  # dnf/yum
  $PKG_MANAGER install -y epel-release || true
  $PKG_MANAGER install -y tor python3 python3-venv python3-pip git || true
fi

# create crawler user if not existing
if ! id "$CRAWLER_USER" >/dev/null 2>&1; then
  echo "Creating system user: $CRAWLER_USER"
  useradd -m -r -s /usr/sbin/nologin "$CRAWLER_USER"
fi

# create project dir
echo "Creating project directory: $PROJECT_DIR"
mkdir -p "$PROJECT_DIR"
chown "$CRAWLER_USER":"$CRAWLER_USER" "$PROJECT_DIR"
chmod 750 "$PROJECT_DIR"

# clone repo if requested (or leave for manual copy)
if [ -n "$GIT_REPO" ]; then
  echo "Cloning repo $GIT_REPO into $PROJECT_DIR"
  # remove existing files only if empty or if it's safe — we assume empty dir
  if [ -n "$(ls -A "$PROJECT_DIR")" ]; then
    echo "Warning: $PROJECT_DIR is not empty. Attempting git pull if it's the same repo."
    # if git repo already exists, try pull
    if [ -d "$PROJECT_DIR/.git" ]; then
      git -C "$PROJECT_DIR" pull || true
    else
      echo "$PROJECT_DIR is not empty and not a git repo. Please clean or set GIT_REPO empty and copy files manually."
    fi
  else
    sudo -u "$CRAWLER_USER" git clone "$GIT_REPO" "$PROJECT_DIR"
  fi
else
  echo "GIT_REPO not set. Please copy your project files (crawler.py, scooper.py, sites.yaml, keywords.txt) into $PROJECT_DIR"
fi

# create venv and install python deps as crawler user
echo "Creating python venv and installing dependencies"
sudo -u "$CRAWLER_USER" bash -c "python3 -m venv '$PROJECT_DIR/venv' || exit 1"
sudo -u "$CRAWLER_USER" bash -c "source '$PROJECT_DIR/venv/bin/activate' && pip install --upgrade pip && pip install $PYTHON_REQUIREMENTS"

# Setup minimal torrc if system tor installed
if command -v tor >/dev/null 2>&1; then
  echo "Configuring Tor at $TORRC_PATH"
  # backup existing torrc
  if [ -f "$TORRC_PATH" ]; then
    cp -n "$TORRC_PATH" "${TORRC_PATH}.bak" || true
  fi
  cat > "$TORRC_PATH" <<'EOF'
SocksPort 9050
ControlPort 9051
CookieAuthentication 1
DataDirectory /var/lib/tor
Log notice file /var/log/tor/notices.log
EOF
  # ensure tor service enabled & started
  systemctl daemon-reload || true
  systemctl enable --now tor || true
  echo "Started/enabled tor service. Check 'journalctl -u tor -f' for bootstrap logs."
else
  echo "System tor not found. If you plan to use the Tor Expert Bundle, install it manually and ensure SocksPort 9050 is available."
fi

# Create systemd unit for crawler
CRAWLER_SERVICE="/etc/systemd/system/paritybit-crawler.service"
echo "Creating systemd service: $CRAWLER_SERVICE"
cat > "$CRAWLER_SERVICE" <<EOF
[Unit]
Description=ParityBit Darkweb Crawler
After=network.target tor.service
Wants=tor.service

[Service]
User=$CRAWLER_USER
Group=$CRAWLER_USER
WorkingDirectory=$PROJECT_DIR
Environment=PATH=$PROJECT_DIR/venv/bin
ExecStart=$PROJECT_DIR/venv/bin/python $PROJECT_DIR/crawler.py --interval-minutes $CRAWLER_INTERVAL_MINUTES
Restart=on-failure
RestartSec=15

[Install]
WantedBy=multi-user.target
EOF

# Create systemd unit for scooper
SCOOPER_SERVICE="/etc/systemd/system/paritybit-scooper.service"
echo "Creating systemd service: $SCOOPER_SERVICE"
cat > "$SCOOPER_SERVICE" <<EOF
[Unit]
Description=ParityBit Scooper (keyword watcher)
After=network.target paritybit-crawler.service
Wants=paritybit-crawler.service

[Service]
User=$CRAWLER_USER
Group=$CRAWLER_USER
WorkingDirectory=$PROJECT_DIR
Environment=PATH=$PROJECT_DIR/venv/bin
ExecStart=$PROJECT_DIR/venv/bin/python $PROJECT_DIR/scooper.py --watch --poll-interval 10
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

# reload systemd, enable services
echo "Reloading systemd and enabling services"
systemctl daemon-reload
systemctl enable --now paritybit-crawler.service paritybit-scooper.service || true

# Fix basic permissions on project dir (crawler owner)
chown -R "$CRAWLER_USER":"$CRAWLER_USER" "$PROJECT_DIR"

# Quick checks
echo
echo "=== Quick checks ==="
echo "Tor status:"
systemctl is-active --quiet tor && echo "tor: active" || echo "tor: not active (check journalctl -u tor)"

echo "paritybit-crawler service:"
systemctl is-active --quiet paritybit-crawler.service && echo "paritybit-crawler: active" || echo "paritybit-crawler: not active (check 'sudo journalctl -u paritybit-crawler -f')"

echo "paritybit-scooper service:"
systemctl is-active --quiet paritybit-scooper.service && echo "paritybit-scooper: active" || echo "paritybit-scooper: not active (check 'sudo journalctl -u paritybit-scooper -f')"

echo
echo "If you did not provide a repo via GIT_REPO, copy your project files (crawler.py, scooper.py, sites.yaml, keywords.txt) into:"
echo "  $PROJECT_DIR"
echo "Then ensure ownership:"
echo "  sudo chown -R $CRAWLER_USER:$CRAWLER_USER $PROJECT_DIR"
echo
echo "To manually run a single test crawl (as the crawler user):"
echo "  sudo -u $CRAWLER_USER bash -lc 'cd $PROJECT_DIR && source venv/bin/activate && python crawler.py --single-run'"
echo
echo "Done. Monitor services with:"
echo "  sudo journalctl -u paritybit-crawler -f"
echo "  sudo journalctl -u paritybit-scooper -f"
