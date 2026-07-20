# Lab on Ubuntu 24.04 (Docker Engine)

Run the Airflow 3.3 + Marquez lab in **native Docker on Ubuntu 24.04**
(Docker Engine, not Docker Desktop). Target topology:

```
┌── Ubuntu 24.04 VM (192.168.1.200) ──┐        ┌── Windows 11 (192.168.1.100) ──┐
│  Pentaho Data Catalog               │        │  Carte / PDI  (:8081)          │
│  Airflow 3.3  (api-server :8088)    │◀──────▶│  Migration Studio (:5012)      │
│  Marquez  (UI :3000 · API :6001)    │  LAN   │                                │
└─────────────────────────────────────┘        └────────────────────────────────┘
```

- Airflow (on the VM) triggers Carte **on the Windows machine** → set
  `CARTE_HOST` to the Windows LAN IP.
- The Studio (on Windows) reaches Airflow / Marquez / PDC **on the VM**
  → point its Settings at `192.168.1.200`.

## Quick start (one command)

On the Ubuntu VM, after cloning the repo:

```bash
git clone https://github.com/jporeilly/PDI-Airflow.git
cd PDI-Airflow
CARTE_HOST=192.168.1.100 ./lab/install-ubuntu.sh
```

That installs Docker Engine if missing, writes `lab/docker/.env`,
opens the firewall, and brings the Airflow 3.3 + Marquez stack up. The
manual steps below explain each part.

## 1. Install Docker Engine on Ubuntu 24.04

```bash
# Docker's official apt repo (not the snap/Desktop)
sudo apt-get update
sudo apt-get install -y ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
     -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] \
  https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo $VERSION_CODENAME) stable" |
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io \
     docker-buildx-plugin docker-compose-plugin
sudo usermod -aG docker "$USER"        # then log out/in (or: newgrp docker)
docker compose version                 # verify
```

## 2. Get the lab onto the VM

Copy the repo (or just the folders the lab needs) to the VM, e.g.:

```bash
# from the Windows machine (Git Bash / scp), or git clone on the VM:
scp -r /c/Projects/PDI-AirFlow user@192.168.1.200:~/PDI-Airflow
```

The lab needs: `lab/`, `airflow-pentaho-provider/`, `workshop/dags/`.

## 3. Configure the network

```bash
cd ~/PDI-Airflow/lab/docker
cp .env.example .env
# set CARTE_HOST to the Windows 11 machine's LAN IP:
sed -i 's/^CARTE_HOST=.*/CARTE_HOST=192.168.1.100/' .env
```

## 4. Start the lab

```bash
docker compose up -d --build        # first run builds the Airflow 3.3 image
docker compose logs -f airflow-apiserver   # watch until healthy
```

Airflow api-server, scheduler, triggerer and dag-processor come up as
separate `restart: unless-stopped` services; the Pentaho + OpenLineage
+ standard + FAB providers are baked into the image.

## 5. Open the firewall (so Windows can reach the VM)

```bash
sudo ufw allow 8088/tcp    # Airflow
sudo ufw allow 3000/tcp    # Marquez UI
sudo ufw allow 6001/tcp    # Marquez API (Studio + PDC publish read it)
```

(PDC already uses 443 / 8000 / 9000 / 9443 on the VM — these don't
clash.)

## 6. Point the Studio (on Windows) at the VM

In the Studio → **Settings**:

| Field | Value |
|---|---|
| Airflow URL | `http://192.168.1.200:8088` |
| Marquez API URL | `http://192.168.1.200:6001` |
| Marquez UI URL | `http://192.168.1.200:3000` |
| PDC URL | `https://pentaho.io` (or `https://192.168.1.200`) |

Then **Test connection** on Airflow and PDC. Airflow uses the **REST
API v2 + JWT** automatically on 3.3 (basic-auth v1 on 2.x).

## Notes

- **Carte reachability**: the Airflow containers reach Carte at
  `CARTE_HOST:CARTE_PORT` over the LAN — make sure the Windows firewall
  allows inbound `8081` from the VM (allow Java on the private network).
- **Marquez lineage**: Airflow → Marquez is internal to the compose
  network; nothing to open for that.
- **One lab only**: if you were running the lab on Windows Docker
  Desktop for testing, `docker compose down` it there so there's a
  single source of truth on the VM.
