#!/bin/bash
# ベンチマーク実行スクリプト（docker compose の環境に対して実行する）
#
#   ./bench.sh quick   簡易チェック: 負荷20秒を1回。不具合（fail）の確認用で、スコアは本計測と比べられない
#   ./bench.sh [N]     本計測: 通常設定（負荷60秒）を N 回（既定3回）、30秒間隔で回して平均を出す
#
# ベンチマーカーは CPU 4〜7 で動かす（アプリ側の割り当ては compose.override.yml）
set -euo pipefail

run_bench() {
  # alp ログは直近1回分だけ残す（消さないと溜まり続けて GB 単位になる）
  docker exec private-isu-nginx-1 sh -c ': > /var/log/nginx/alp.log'
  docker run --rm --network private-isu_my_network --cpuset-cpus=4-7 private-isu-benchmarker \
    /bin/benchmarker -t http://nginx -u /opt/userdata "$@" 2>/dev/null | tail -1
}

# 1行の JSON から score・fail と、先頭のエラーメッセージ（最大5件）を表示する
show() {
  python3 -I -c '
import json, sys
r = json.loads(sys.argv[1])
score, success, fail, ok, msgs = r["score"], r["success"], r["fail"], r["pass"], r["messages"]
print(f"score={score:>9,}  success={success:,}  fail={fail}  pass={ok}")
for m in msgs[:5]:
    print("    " + m)
if len(msgs) > 5:
    print(f"    ...ほか {len(msgs) - 5} 件")
' "$1"
}

if [ "${1:-}" = "quick" ]; then
  show "$(run_bench -benchmark-timeout 20s -wait-after-timeout 2s)"
  exit
fi

n=${1:-3}
scores=()
for i in $(seq 1 "$n"); do
  # 前回の負荷の影響が残るとスコアが安定しないので、計測ごとに30秒あける
  sleep 30
  result=$(run_bench)
  printf '[%d/%d] ' "$i" "$n"
  show "$result"
  scores+=("$(python3 -I -c 'import json, sys; print(json.loads(sys.argv[1])["score"])' "$result")")
done

python3 -I -c '
import sys
s = [int(x) for x in sys.argv[1:]]
print(f"平均 {sum(s) // len(s):,}  (最小 {min(s):,} / 最大 {max(s):,})")
' "${scores[@]}"
