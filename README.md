## One-command install (recommended)

Share this command with customers. It downloads and runs the installer script:

```bash
curl -fsSL https://raw.githubusercontent.com/simpledotorg/h360tk_india/main/scripts/install.sh | bash
```

What it does:
- Clones/updates this repo on the target machine (`$HOME/h360tk_india` by default)
- Prompts for required env values and writes `.env`
- Runs `docker compose pull` and `docker compose up -d --remove-orphans`

Optional overrides:

```bash
INSTALL_DIR=/opt/h360tk_india BRANCH=main \
curl -fsSL https://raw.githubusercontent.com/simpledotorg/h360tk_india/main/scripts/install.sh | bash
```

It requires Docker and Docker Compose plugin to already be installed.

## Access URLs

- Dashboard: http://localhost:3000/d/heart360demo/heart-360-global-dashboard
- File upload: http://localhost:8080/
