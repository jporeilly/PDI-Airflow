#!/usr/bin/env bash
# One-command lab bootstrap for Ubuntu 24.04 — installs Docker Engine
# (if missing), configures the Carte host, and brings up the Airflow 3.3
# + Marquez stack. See lab/UBUNTU-SETUP.md for the full walkthrough.
#
#   ./lab/install-ubuntu.sh                       # prompts for CARTE_HOST
#   CARTE_HOST=192.168.1.100 ./lab/install-ubuntu.sh   # non-interactive
#
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
docker_dir="$here/docker"

say() { printf '\033[36m==> %s\033[0m\n' "$*"; }
ok()  { printf '\033[32m    %s\033[0m\n' "$*"; }

# 1. Docker Engine ---------------------------------------------------------
if ! command -v docker >/dev/null 2>&1; then
  say "Installing Docker Engine (Ubuntu apt repo)..."
  sudo apt-get update
  sudo apt-get install -y ca-certificates curl
  sudo install -m 0755 -d /etc/apt/keyrings
  sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
       -o /etc/apt/keyrings/docker.asc
  sudo chmod a+r /etc/apt/keyrings/docker.asc
  echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] \
https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" |
    sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
  sudo apt-get update
  sudo apt-get install -y docker-ce docker-ce-cli containerd.io \
       docker-buildx-plugin docker-compose-plugin
  sudo usermod -aG docker "$USER" || true
  ok "Docker installed. If 'permission denied' below, log out/in (docker group) and re-run."
else
  ok "Docker already present: $(docker --version)"
fi

# 2. .env (Carte host) -----------------------------------------------------
env_file="$docker_dir/.env"
if [ ! -f "$env_file" ]; then
  cp "$docker_dir/.env.example" "$env_file"
  say "Created $env_file from the example."
fi
if [ -z "${CARTE_HOST:-}" ]; then
  read -rp "    Carte/PDI host (the Windows machine's LAN IP) [192.168.1.100]: " CARTE_HOST
  CARTE_HOST="${CARTE_HOST:-192.168.1.100}"
fi
sed -i "s/^CARTE_HOST=.*/CARTE_HOST=${CARTE_HOST}/" "$env_file"
ok "CARTE_HOST=${CARTE_HOST}"

# 3. Firewall (best effort) ------------------------------------------------
if command -v ufw >/dev/null 2>&1 && sudo ufw status | grep -q active; then
  say "Opening lab ports (8088 Airflow, 3000/6001 Marquez)..."
  for p in 8088 3000 6001; do sudo ufw allow "${p}/tcp" >/dev/null || true; done
  ok "Firewall rules added."
fi

# 4. Build + up ------------------------------------------------------------
say "Building the Airflow 3.3 image and starting the stack..."
( cd "$docker_dir" && docker compose up -d --build )

host_ip="$(hostname -I 2>/dev/null | awk '{print $1}')"
echo
ok "Lab is starting. From the Windows Studio, point Settings at this host:"
ok "  Airflow    http://${host_ip:-<this-vm-ip>}:8088   (admin / admin)"
ok "  Marquez UI http://${host_ip:-<this-vm-ip>}:3000"
ok "  Marquez API http://${host_ip:-<this-vm-ip>}:6001"
ok "Watch it come up:  cd lab/docker && docker compose logs -f airflow-apiserver"
