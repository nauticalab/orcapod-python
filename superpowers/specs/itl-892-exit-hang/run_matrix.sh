#!/usr/bin/env bash
# ITL-892 macOS matrix. Run from the repository root:
#   bash superpowers/specs/itl-892-exit-hang/run_matrix.sh <group> [N]
# Groups: ops, readpath, threads, mitigations, python, versions, orcapod,
#         orcapod-python, orcapod-patches.
# Each cell appends one JSON line to superpowers/specs/itl-892-exit-hang/results.jsonl.
set -u
D=superpowers/specs/itl-892-exit-hang
N=${2:-100}
J=${J:-10}
T=${T:-8}
OP=${OP:-dataset_scan}
H() { uv run python $D/harness.py -n "$N" -t "$T" -j "$J" "$@"; }
# Throwaway environment with chosen versions; uv.lock is untouched.
V() { # V <py> <pyarrow> <deltalake> <label> -- args...
  local py=$1 pa=$2 dl=$3 label=$4; shift 5
  uv run --no-project --python "$py" --with "pyarrow==$pa" --with "deltalake==$dl" \
    python $D/harness.py -n "$N" -t "$T" -j "$J" --label "$label" -- \
    python $D/repro_pure.py "$@"
}

case "$1" in
ops)
  for op in pyarrow_only write merge to_dataset to_table dataset_scan itl864; do
    H --label "op=$op py3.12 locked" -- python $D/repro_pure.py --op "$op"
  done ;;
readpath)
  for rv in native_fs no_threads querybuilder files polars; do
    H --label "$OP read-via=$rv py3.12 locked" -- python $D/repro_pure.py --op "$OP" --read-via "$rv"
  done ;;
threads)
  H --label "$OP cpu_threads=1" -- python $D/repro_pure.py --op "$OP" --cpu-threads 1
  H --label "$OP io_threads=1" -- python $D/repro_pure.py --op "$OP" --io-threads 1
  OMP_NUM_THREADS=1 H --label "$OP OMP_NUM_THREADS=1" -- python $D/repro_pure.py --op "$OP"
  RAYON_NUM_THREADS=1 TOKIO_WORKER_THREADS=1 H --label "$OP RAYON/TOKIO threads=1" -- python $D/repro_pure.py --op "$OP"
  H --label "$OP import polars after pyarrow" -- python $D/repro_pure.py --op "$OP" --import-polars
  H --label "$OP import polars first" -- python $D/repro_pure.py --op "$OP" --polars-first
  H --label "$OP in fork child" -- python $D/repro_pure.py --op "$OP" --mp fork
  H --label "$OP in spawn child" -- python $D/repro_pure.py --op "$OP" --mp spawn ;;
mitigations)
  for m in ${MITS:-del atexit_del os_exit atexit_sleep atexit_sleep_long}; do
    H --label "$OP mitigation=$m" -- python $D/repro_pure.py --op "$OP" --mitigation "$m"
  done ;;
python)
  for py in 3.11 3.12 3.13; do
    V "$py" 23.0.1 1.0.2 "$OP py$py pa23.0.1 dl1.0.2" -- --op "$OP"
    V "$py" 23.0.1 1.0.2 "$OP native_fs py$py pa23.0.1 dl1.0.2" -- --op "$OP" --read-via native_fs
  done ;;
versions)
  for pa in ${PA:-20.0.0 22.0.0 23.0.1 24.0.0 25.0.1 26.0.1}; do
    for dl in ${DL:-1.0.2 1.6.6}; do
      V 3.12 "$pa" "$dl" "$OP py3.12 pa$pa dl$dl" -- --op "$OP"
    done
  done ;;
orcapod)
  for m in ${MODES:-plain-read source-read pipeline-sync pipeline-async rerun}; do
    H --label "orcapod $m py3.12 locked" -- python $D/repro_orcapod.py "$m"
  done ;;
orcapod-python)
  # Locked versions, project installed in a throwaway environment per interpreter.
  for py in 3.11 3.13; do
    for m in ${MODES:-plain-read pipeline-sync pipeline-async}; do
      uv run --isolated --frozen --python "$py" python $D/harness.py -n "$N" -t "$T" -j "$J" \
        --label "orcapod $m py$py locked" -- python $D/repro_orcapod.py "$m"
    done
  done ;;
orcapod-patches)
  # Options (b)/(c) prototyped by wrapping deltalake in the repro; src/ unchanged.
  for p in ${PATCHES:-native-fs no-threads atexit-gc}; do
    for m in ${MODES:-plain-read source-read pipeline-sync pipeline-async}; do
      H --label "orcapod $m patch=$p py3.12 locked" -- python $D/repro_orcapod.py "$m" --patch "$p"
    done
  done ;;
*) echo "unknown group $1"; exit 2 ;;
esac
