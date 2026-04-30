# ============================================================
# distributed_training/pipeline_orchestrator.py
# Bộ điều phối chạy lần lượt 5 baseline model
# ============================================================

import subprocess
import os
import sys
from config.cluster_config import ClusterConfig

def run_command(command):
    print(f"Đang thực thi: {command}")
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    for line in iter(process.stdout.readline, b''):
        sys.stdout.write(line.decode('utf-8'))
    process.communicate()
    return process.returncode

def main():
    print("--- BẮT ĐẦU PIPELINE HUẤN LUYỆN 5 BASELINE ---")
    
    # 1. BASELINE 1: BM25 (Thường chạy job Spark để tính score)
    print("\n[STEP 1] Running BM25 Indexing...")
    # run_command("python scripts/run_bm25_spark.py")

    # 2. BASELINE 2: SBERT (Distributed Training)
    print("\n[STEP 2] Training SBERT Baseline (Multi-node DDP)...")
    # Lệnh chạy torchrun đồng bộ trên các node
    torchrun_cmd = (
        f"torchrun --nproc_per_node=1 --nnodes={ClusterConfig.WORLD_SIZE} "
        f"--rdzv_id=101 --rdzv_backend=c10d --rdzv_endpoint={ClusterConfig.MASTER_ADDR}:{ClusterConfig.MASTER_PORT} "
        "train_sbert.py --batch_size 16 --epochs 5"
    )
    # run_command(torchrun_cmd)

    # 3. BASELINE 3: DSSM
    print("\n[STEP 3] Training DSSM Baseline...")
    # run_command("torchrun ... train_dssm.py")

    # 4. BASELINE 4: GCN
    print("\n[STEP 4] Training GCN Baseline...")
    # run_command("torchrun ... train_gcn.py")

    # 5. BASELINE 5: HYBRID
    print("\n[STEP 5] Combining results for HYBRID Baseline...")
    # run_command("python scripts/run_hybrid.py")

    print("\n--- TẤT CẢ 5 BASELINE ĐÃ HOÀN TẤT! ---")

if __name__ == "__main__":
    main()
