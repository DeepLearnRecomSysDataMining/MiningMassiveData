import argparse
import logging
import sys
import os
import torch
import torch.distributed as dist

# Thêm thư mục hiện tại vào sys.path để import được các module nội bộ
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config.training_config import TrainingConfig, setup_logging
from src.gcs_manager import download_training_data, upload_model_checkpoint
from src.data_utils import load_eval_dataset, load_interactions_df, load_precomputed_embeddings

# Import các baseline
from src.baselines.bm25_ranker import run_bm25
from src.baselines.sbert_ranker import run_sbert
from src.baselines.dssm_trainer import train_dssm
from src.baselines.gcn_trainer import train_gcn
from src.baselines.hybrid_ranker import run_hybrid
from src.baselines.llm_chgnn_trainer import run_llm_chgnn
from src.precompute_embeddings import precompute_item_embeddings

logger = logging.getLogger("training_main")

def setup_distributed():
    if not dist.is_initialized():
        backend = "nccl" if torch.cuda.is_available() else "gloo"
        if torch.cuda.is_available():
            torch.cuda.set_device(TrainingConfig.LOCAL_RANK)
        dist.init_process_group(backend=backend, init_method="env://")

def cleanup_distributed():
    if dist.is_initialized():
        dist.destroy_process_group()

def sync_rank0_stage(stage_name, timeout=21600):
    import time
    flag_path = f"/tmp/{stage_name}_done.flag"

    if TrainingConfig.RANK == 0:
        with open(flag_path, "w") as f:
            f.write("done")
        return

    elapsed = 0
    while not os.path.exists(flag_path) and elapsed < timeout:
        time.sleep(10)
        elapsed += 10

    if not os.path.exists(flag_path):
        raise TimeoutError(f"Timeout waiting for Rank 0 stage: {stage_name}")

def run_pipeline(baseline_id):
    ckpt_path = None
    metrics_path = None

    # Baseline không cần multi-GPU: chỉ Rank 0 chạy
    if baseline_id in [1, 2, 5, 6] and TrainingConfig.RANK != 0:
        return

    if baseline_id == 1:        
        eval_dataset = load_eval_dataset()
        run_bm25(eval_dataset)
        metrics_path = os.path.join(TrainingConfig.LOCAL_MODELS_DIR, "bm25_metrics.csv")
        
    elif baseline_id == 2:
        eval_dataset = load_eval_dataset()
        run_sbert(eval_dataset)
        metrics_path = os.path.join(TrainingConfig.LOCAL_MODELS_DIR, "sbert_metrics.csv")

    elif baseline_id in [3, 4]:
        # 1. KIỂM TRA LOCAL VÀ GCS
        emb_path = TrainingConfig.ITEM_EMBEDDINGS_PATH
        if not os.path.exists(emb_path):
            if TrainingConfig.RANK == 0:
                logger.info("!!! KHÔNG TÌM THẤY EMBEDDINGS LOCAL. ĐANG KÍCH HOẠT PRECOMPUTE...")
            
            # Chạy Precompute (Sử dụng 4 GPU - Tự động Resume và Gộp file)
            precompute_item_embeddings()
            
            # Sau khi xong, tất cả các Rank sẽ tự động đồng bộ qua barrier nội bộ của hàm trên
            # Dữ liệu lúc này đã sẵn sàng ở LOCAL_DATA_DIR cho các bước tiếp theo
        
        # 2. Nạp dữ liệu Vector (Memory-Mapped)
        embedding_lookup = load_precomputed_embeddings()
        interactions_df = load_interactions_df()
        
        if baseline_id == 3:
            ckpt_path = train_dssm(interactions_df, embedding_lookup)
            metrics_path = os.path.join(TrainingConfig.LOCAL_MODELS_DIR, "dssm_metrics.csv")
        else:
            ckpt_path = train_gcn(interactions_df, embedding_lookup)
            metrics_path = os.path.join(TrainingConfig.LOCAL_MODELS_DIR, "gcn_metrics.csv")
            
    elif baseline_id == 5:
        eval_dataset = load_eval_dataset()
        run_hybrid(eval_dataset)
        metrics_path = os.path.join(TrainingConfig.LOCAL_MODELS_DIR, "hybrid_metrics.csv")
        
    elif baseline_id == 6:
        eval_dataset = load_eval_dataset()
        run_llm_chgnn(eval_dataset)
        metrics_path = os.path.join(TrainingConfig.LOCAL_MODELS_DIR, "llm_chgnn_metrics.csv")

    if TrainingConfig.RANK == 0 and ckpt_path and os.path.exists(ckpt_path):
        try:
            logger.info(f"Uploading checkpoint {ckpt_path} to GCS...")
            upload_model_checkpoint(ckpt_path)
        except Exception as e:
            logger.error(f"\n\n\nFailed to upload checkpoint {ckpt_path}: {e}\n\n\n")

    if TrainingConfig.RANK == 0 and metrics_path and os.path.exists(metrics_path):
        try:
            logger.info(f"Uploading metrics {metrics_path} to GCS...")
            upload_model_checkpoint(metrics_path)
        except Exception as e:
            logger.error(f"\n\n\nFailed to upload metrics {metrics_path}: {e}\n\n\n")

def main():
    setup_logging()
    parser = argparse.ArgumentParser(description="RecSys Multi-Node Multi-GPU Pipeline")
    parser.add_argument("--baseline", type=str, default="all", help="Chọn Baseline (1-6) hoặc 'all'")
    parser.add_argument("--skip-download", action="store_true", help="Bỏ qua tải dữ liệu từ GCS")
    args = parser.parse_args()

    baseline_arg = str(args.baseline)
    needs_distributed = baseline_arg in ["3", "4", "all"]

    # Chỉ baseline 3, 4, all mới cần distributed
    if needs_distributed:
        setup_distributed()
    else:
        # Baseline 1,2,5,6 chạy single process trên GPU 0 nếu có
        if torch.cuda.is_available():
            torch.cuda.set_device(0)

    try:
        # 1. ĐỒNG BỘ DỮ LIỆU (Chỉ GPU 0 tải, các GPU khác đợi để tránh xung đột gsutil)
        if TrainingConfig.RANK == 0:
            download_training_data()
            if needs_distributed:
                sync_rank0_stage("download_data")
        else:
            if needs_distributed:
                sync_rank0_stage("download_data")

        if TrainingConfig.RANK == 0:
            print("\n" + "="*60)
            print(f"   AMAZON x VN - SUPER-FAST DISTRIBUTED TRAINING (PRECOMPUTED)")
            print(f"   World Size: {TrainingConfig.WORLD_SIZE} | Mode: {args.baseline}")
            print("="*60 + "\n")

        # Với baseline 1,2,5,6 thì không cần đợi item_embeddings.npy
        # Vì chỉ baseline 3,4 mới cần precomputed embeddings

        if args.baseline == "all":
            baselines_to_run = [1, 2, 3, 4, 5, 6]
        else:
            baselines_to_run = [int(args.baseline)]

        for b_id in baselines_to_run:
            if TrainingConfig.RANK == 0: 
                logger.info(f">>> BẮT ĐẦU BASELINE {b_id} <<<")
            try: 
                run_pipeline(b_id)
            except Exception as e: 
                logger.error(f"Thất bại tại Baseline {b_id}: {e}")
                import traceback
                traceback.print_exc()
            # Khi chạy all, baseline 1/2/5/6 chỉ Rank 0 chạy,
            # Rank khác phải đợi Rank 0 xong rồi mới đi tiếp.
            if needs_distributed and b_id in [1, 2, 5, 6]:
                sync_rank0_stage(f"baseline_{b_id}")
            
            # Baseline 3/4 chạy multi-GPU thật.
            if dist.is_initialized() and b_id in [3, 4]:
                dist.barrier()

        if TrainingConfig.RANK == 0: 
            logger.info("TOÀN BỘ PIPELINE ĐÃ HOÀN TẤT!")

    finally:
        cleanup_distributed()

if __name__ == "__main__":
    main()
