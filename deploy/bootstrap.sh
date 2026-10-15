#!/usr/bin/env bash
# Prepares a fresh Debian 12+ / Ubuntu 24.04+ VPS to receive releases.
# Safe to re-run after changing anything in deploy/.
#
#   sudo deploy/bootstrap.sh --domain polyviz.example.com --login me \
#     --deploy-key "$(cat polyviz-deploy.pub)" [--password-hash '$2a$14$...']
#
# Without --password-hash, Caddy prompts for the dashboard password.
set -euo pipefail

here=$(dirname "$(readlink -f "$0")")
domain='' login='' deploy_key='' password_hash=''

while (($#)); do
  case $1 in
    --domain) domain=$2; shift 2 ;;
    --login) login=$2; shift 2 ;;
    --deploy-key) deploy_key=$2; shift 2 ;;
    --password-hash) password_hash=$2; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done
[[ -n $domain && -n $login && -n $deploy_key ]] || {
  echo "required: --domain, --login, --deploy-key" >&2
  exit 2
}
[[ $EUID -eq 0 ]] || { echo "run as root" >&2; exit 1; }

export DEBIAN_FRONTEND=noninteractive
apt-get update -q
apt-get install -yq curl ca-certificates gnupg debian-keyring debian-archive-keyring \
  apt-transport-https sqlite3 sudo ufw

if ! command -v caddy >/dev/null; then
  curl -1sLf https://dl.cloudsmith.io/public/caddy/stable/gpg.key |
    gpg --batch --yes --dearmor -o /usr/share/keyrings/caddy-stable-archive-keyring.gpg
  chmod o+r /usr/share/keyrings/caddy-stable-archive-keyring.gpg
  curl -1sLf https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt \
    >/etc/apt/sources.list.d/caddy-stable.list
  apt-get update -q
  apt-get install -yq caddy
fi

# polyviz runs the recorder; polyviz-deploy owns the release trees and can
# only restart the recorder.
id polyviz &>/dev/null ||
  useradd --system --user-group --home-dir /var/lib/polyviz --no-create-home \
    --shell /usr/sbin/nologin polyviz
id polyviz-deploy &>/dev/null ||
  useradd --create-home --user-group --shell /bin/bash polyviz-deploy

install -d -m 755 /opt/polyviz
for component in recorder frontend; do
  install -d -m 755 -o polyviz-deploy -g polyviz-deploy \
    "/opt/polyviz/$component" "/opt/polyviz/$component/releases"
done

install -m 755 "$here/polyviz-deploy" /usr/local/bin/polyviz-deploy
install -m 644 "$here/polyviz-recorder.service" /etc/systemd/system/polyviz-recorder.service
systemctl daemon-reload
systemctl enable polyviz-recorder.service

sudoers=$(mktemp)
echo "polyviz-deploy ALL=(root) NOPASSWD: $(command -v systemctl) restart polyviz-recorder.service" >"$sudoers"
visudo -cqf "$sudoers"
install -m 440 "$sudoers" /etc/sudoers.d/polyviz-deploy
rm -f "$sudoers"

deploy_home=$(getent passwd polyviz-deploy | cut -d: -f6)
install -d -m 700 -o polyviz-deploy -g polyviz-deploy "$deploy_home/.ssh"
printf 'restrict,command="/usr/local/bin/polyviz-deploy" %s\n' "$deploy_key" \
  >"$deploy_home/.ssh/authorized_keys"
chown polyviz-deploy:polyviz-deploy "$deploy_home/.ssh/authorized_keys"
chmod 600 "$deploy_home/.ssh/authorized_keys"

if [[ -z $password_hash ]]; then
  echo "Choose the dashboard password for $login:"
  password_hash=$(caddy hash-password)
fi
caddyfile=$(mktemp)
sed -e "s|@DOMAIN@|$domain|" -e "s|@LOGIN@|$login|" -e "s|@PASSWORD_HASH@|$password_hash|" \
  "$here/Caddyfile.in" >"$caddyfile"
caddy validate --adapter caddyfile --config "$caddyfile"
install -m 644 "$caddyfile" /etc/caddy/Caddyfile
rm -f "$caddyfile"
systemctl reload-or-restart caddy

ufw allow 22/tcp
ufw allow 80/tcp
ufw allow 443/tcp
ufw --force enable

cat <<EOF

Server ready. In both GitHub repositories, create a "production" environment with
  variable DEPLOY_HOST        = $domain
  variable DEPLOY_KNOWN_HOSTS = output of: ssh-keyscan -t ed25519 $domain
  secret   DEPLOY_SSH_KEY     = the private half of the deploy key
then publish a release (or re-run the deploy job of an existing one).
EOF
