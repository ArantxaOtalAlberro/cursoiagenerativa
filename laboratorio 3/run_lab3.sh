#!/usr/bin/env bash
# run_lab3.sh - Ejecuta el laboratorio 3
# Uso: ./run_lab3.sh [NOMBRE] [NUMERO]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

NAME=""
NUMBER=""

if [ $# -ge 1 ]; then
  NAME="$1"
fi
if [ $# -ge 2 ]; then
  NUMBER="$2"
fi

if [ -n "$NAME" ] && [ -n "$NUMBER" ]; then
  # Ejecuta pasando las entradas por heredoc para evitar interacción
  python main.py <<EOF
$NAME
$NUMBER
EOF
else
  # Ejecuta interactivamente
  python main.py
fi
