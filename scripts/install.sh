#!/usr/bin/env bash
set -euo pipefail

REPO_URL="${REPO_URL:-https://github.com/simpledotorg/h360tk_india.git}"
INSTALL_DIR="${INSTALL_DIR:-$HOME/h360tk_india}"
BRANCH="${BRANCH:-main}"

log() { printf "[h360tk] %s\n" "$*"; }
err() { printf "[h360tk][error] %s\n" "$*" >&2; }

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    err "Missing required command: $1"
    exit 1
  fi
}

ensure_docker_compose() {
  if docker compose version >/dev/null 2>&1; then
    return
  fi
  err "Docker Compose plugin is not available. Install Docker + Compose first."
  exit 1
}

prompt_if_empty() {
  local var_name="$1"
  local prompt_text="$2"
  local default_value="${3:-}"
  local current_value="${!var_name:-}"

  if [ -n "$current_value" ]; then
    return
  fi

  if [ -n "$default_value" ]; then
    read -r -p "$prompt_text [$default_value]: " current_value || true
    current_value="${current_value:-$default_value}"
  else
    read -r -p "$prompt_text: " current_value || true
  fi

  if [ -z "$current_value" ]; then
    err "$var_name cannot be empty"
    exit 1
  fi

  printf -v "$var_name" "%s" "$current_value"
}

write_env_file() {
  local env_file="$1"
  cat >"$env_file" <<EOF
HEART360TK_VERSION=$HEART360TK_VERSION
POSTGRES_DB=$POSTGRES_DB
POSTGRES_HOST=$POSTGRES_HOST
EOF
}

main() {
  require_cmd git
  require_cmd docker
  ensure_docker_compose

  log "Installing to: $INSTALL_DIR"
  mkdir -p "$INSTALL_DIR"

  if [ -d "$INSTALL_DIR/.git" ]; then
    log "Existing repo detected. Pulling latest changes from $BRANCH"
    git -C "$INSTALL_DIR" fetch origin "$BRANCH"
    git -C "$INSTALL_DIR" checkout "$BRANCH"
    git -C "$INSTALL_DIR" pull --ff-only origin "$BRANCH"
  else
    if [ -n "$(ls -A "$INSTALL_DIR" 2>/dev/null || true)" ]; then
      err "Install directory is not empty: $INSTALL_DIR"
      err "Set INSTALL_DIR to an empty path and try again."
      exit 1
    fi
    git clone --branch "$BRANCH" --single-branch "$REPO_URL" "$INSTALL_DIR"
  fi

  prompt_if_empty HEART360TK_VERSION "Enter HEART360TK_VERSION image tag" "latest"
  prompt_if_empty POSTGRES_DB "Enter POSTGRES_DB name" "db_prod"
  prompt_if_empty POSTGRES_HOST "Enter POSTGRES_HOST" "postgres"

  mkdir -p "$INSTALL_DIR/.upload" "$INSTALL_DIR/.database"
  write_env_file "$INSTALL_DIR/.env"
  log "Wrote $INSTALL_DIR/.env"

  log "Pulling images"
  docker compose -f "$INSTALL_DIR/docker-compose.yml" --env-file "$INSTALL_DIR/.env" pull

  log "Starting services"
  docker compose -f "$INSTALL_DIR/docker-compose.yml" --env-file "$INSTALL_DIR/.env" up -d --remove-orphans

  log "Deployment complete."
  log "Grafana: http://localhost:3000/d/heart360demo/heart-360-global-dashboard"
  log "File upload: http://localhost:8080/"
}

main "$@"

