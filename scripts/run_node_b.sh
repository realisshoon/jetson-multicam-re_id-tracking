#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
venv_python="$repo_root/.venv/bin/python"
mqtt_config="$repo_root/configs/mqtt.yaml"
node_entrypoint="$repo_root/src/nodes/node_b.py"
camera_device="/dev/video0"

if [[ ! -x "$venv_python" ]]; then
    echo "Node B 실행 실패: repo-local Python이 없습니다: $venv_python" >&2
    exit 1
fi

if [[ ! -f "$mqtt_config" ]]; then
    echo "Node B 실행 실패: MQTT 설정 파일이 없습니다: $mqtt_config" >&2
    exit 1
fi

if [[ ! -f "$node_entrypoint" ]]; then
    echo "Node B 실행 실패: Python entrypoint가 없습니다: $node_entrypoint" >&2
    exit 1
fi

if [[ ! -e "$camera_device" ]]; then
    echo "Node B 실행 실패: 카메라 장치가 없습니다: $camera_device" >&2
    exit 1
fi

cd "$repo_root"
echo "Node B Python: $venv_python"
exec "$venv_python" -m src.nodes.node_b "$@"
