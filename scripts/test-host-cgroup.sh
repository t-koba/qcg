#!/usr/bin/env bash
# Run only inside a systemd unit delegated to the test UID.
set -euo pipefail
test "$(id -u)" -ne 0 # This job proves runner-UID delegation, never root-only permissions.
RELATIVE=$(awk -F: '$1 == "0" {print $3}' /proc/self/cgroup)
[[ "$RELATIVE" == /* && "$RELATIVE" != / ]]
export QCG_HOST_CGROUP_PARENT="/sys/fs/cgroup$RELATIVE"
test -w "$QCG_HOST_CGROUP_PARENT/cgroup.procs"
PROBE="$QCG_HOST_CGROUP_PARENT/qcg-probe-$$"
mkdir "$PROBE"
test -w "$PROBE/cgroup.procs"
test -w "$PROBE/cgroup.kill"
grep -qx 'populated 0' "$PROBE/cgroup.events"
rmdir "$PROBE"
printf 'Delegation tests enabled: uid=%s parent=%s\n' "$(id -u)" "$QCG_HOST_CGROUP_PARENT"
cargo test --offline --locked -p policy --lib host_cgroup -- --nocapture
cargo test --offline --locked -p engine --lib host_cgroup -- --nocapture
cargo test --offline --locked -p engine --test host_cgroup_crash -- --nocapture
cargo test --offline --locked -p mcp --lib host_cgroup -- --nocapture
cargo test --offline --locked -p service --lib host_cgroup -- --nocapture
cargo test --offline --locked -p cli --test direct_run_recovery sigkill_retains_started_operation_before_restart_or_delete -- --nocapture
