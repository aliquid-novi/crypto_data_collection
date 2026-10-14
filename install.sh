#!/usr/bin/env bash
# One-time VM setup. Review before running. Usage: sudo ops/install.sh <unix-user>
set -euo pipefail
U=${1:?unix user that owns /opt/md-capture}
apt-get install -y chrony rclone python3-venv
systemctl enable --now chrony
chronyc tracking | sed -n '1,6p'   # check: System time offset should be well under 1 ms
cd /opt/md-capture
sudo -u "$U" python3 -m venv .venv
sudo -u "$U" .venv/bin/pip install -e .
for u in ops/systemd/*.service ops/systemd/*.timer; do
  n=$(basename "$u"); base=${n%.*}; ext=${n##*.}
  cp "$u" "/etc/systemd/system/${base}@.${ext}"
done
systemctl daemon-reload
echo "Now: systemctl enable --now mdcap-collector@$U.service mdcap-normalize@$U.timer mdcap-deliver@$U.timer mdcap-audit@$U.timer"
echo "(only after at least one market is 'active' - python -m mdcap validate)"
