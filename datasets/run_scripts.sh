#!/bin/bash

SCRIPT_DIR=$(dirname "$(realpath "$0")")
PROJECT_ROOT=$(realpath "$SCRIPT_DIR/..")
export PYTHONPATH="$PROJECT_ROOT"

scripts=("01_v2022_node_metrics.py" "02_iwqos23.py" "03_node_table.py" "04_edge_table.py")

START_TIME=$(date +%s)

for script in "${scripts[@]}"; do
    echo "Running $script"
    python "$SCRIPT_DIR/$script"

    if [ $? -ne 0 ]; then
        echo "Error: $script failed to run successfully!" >&2
        exit 1
    fi

    echo "$script completed successfully."
    echo "--------------------------------------------"
done

END_TIME=$(date +%s)

TOTAL_TIME=$((END_TIME - START_TIME))

echo "Total time taken: $TOTAL_TIME seconds"
