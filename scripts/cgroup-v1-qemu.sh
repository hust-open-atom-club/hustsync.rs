#!/usr/bin/env bash
# Runs the cgroup root tests under cgroup v1, in a qemu guest booting Ubuntu 24.04's kernel.
# Needs: qemu-system-x86, busybox-static, cpio, dpkg-deb and the x86_64-unknown-linux-musl target.
set -euo pipefail

kver=6.8.0-124
cache=${XDG_CACHE_HOME:-$HOME/.cache}/hustsync-cgroup-v1
cd "$(dirname "$0")/.."

mkdir -p "$cache"
if [[ ! -s $cache/vmlinuz ]]; then
  deb=linux-image-unsigned-$kver-generic_$kver.${kver##*-}_amd64.deb
  curl -fsSL -o "$cache/kernel.deb" "http://archive.ubuntu.com/ubuntu/pool/main/l/linux/$deb"
  dpkg-deb --fsys-tarfile "$cache/kernel.deb" | tar -xO --wildcards './boot/vmlinuz-*' >"$cache/vmlinuz"
fi

bin=$(cargo test -p hustsync-worker --test cgroup_root --target x86_64-unknown-linux-musl \
  --no-run --message-format=json | grep -o '"executable":"[^"]*cgroup_root[^"]*"' | cut -d'"' -f4)

root=$(mktemp -d)
trap 'rm -rf "$root"' EXIT
mkdir -p "$root"/{bin,proc,sys,dev,tmp}
cp "$(command -v busybox)" "$root/bin/"
cp "$bin" "$root/cgtest"
cat >"$root/init" <<'INIT'
#!/bin/busybox sh
/bin/busybox --install -s /bin
export PATH=/bin
mount -t proc proc /proc
mount -t sysfs sys /sys
mount -t devtmpfs dev /dev
mount -t tmpfs tmp /tmp
mount -t tmpfs cgroup /sys/fs/cgroup
mkdir /sys/fs/cgroup/memory
mount -t cgroup -o memory memory /sys/fs/cgroup/memory
/cgtest --ignored --test-threads=1
echo "TEST_RC=$?"
poweroff -f
INIT
chmod +x "$root/init"
(cd "$root" && find . | cpio -o -H newc --quiet | gzip -1) >"$cache/initrd.gz"

# Hosted runners install amd64v3 packages, so busybox needs AVX in the guest.
out=$(timeout 300 qemu-system-x86_64 -accel kvm -accel tcg -cpu max -m 1024 -smp 2 -nographic -no-reboot \
  -kernel "$cache/vmlinuz" -initrd "$cache/initrd.gz" -append "console=ttyS0 panic=-1 quiet" |
  tr -d '\r' | tee /dev/stderr)
grep -qx 'TEST_RC=0' <<<"$out"
