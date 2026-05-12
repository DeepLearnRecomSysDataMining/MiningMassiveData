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
        dist.init_process_group(backend=backend, init_method="env://")
        if torch.cuda.is_available():
            torch.cuda.set_device(TrainingConfig.DEVICE)
        dist.barrier()

def cleanup_distributed():
    if dist.is_initialized():
        dist.destroy_process_group()

def run_pipeline(baseline_id):
    ckpt_path = None

    if baseline_id == 1:
        eval_dataset = load_eval_dataset()
        run_bm25(eval_dataset)
    elif baseline_id == 2:
        eval_dataset = load_eval_dataset()
        run_sbert(eval_dataset)
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
        else:
            ckpt_path = train_gcn(interactions_df, embedding_lookup)
            
    elif baseline_id == 5:
        eval_dataset = load_eval_dataset()
        run_hybrid(eval_dataset)
    elif baseline_id == 6:
        eval_dataset = load_eval_dataset()
        run_llm_chgnn(eval_dataset)

    if TrainingConfig.RANK == 0 and ckpt_path and os.path.exists(ckpt_path):
        logger.info(f"Uploading checkpoint {ckpt_path} to GCS...")
        upload_model_checkpoint(ckpt_path)

def main():
    setup_logging()
    parser = argparse.ArgumentParser(description="RecSys Multi-Node Multi-GPU Pipeline")
    parser.add_argument("--baseline", type=str, default="all", help="Chọn Baseline (1-6) hoặc 'all'")
    parser.add_argument("--skip-download", action="store_true", help="Bỏ qua tải dữ liệu từ GCS")
    args = parser.parse_args()

    setup_distributed()

    # 1. ĐỒNG BỘ DỮ LIỆU (Chỉ GPU 0 tải, các GPU khác đợi để tránh xung đột gsutil)
    if TrainingConfig.RANK == 0:
        download_training_data()
    else:
        import time
        # Đợi tối đa 5 phút cho việc tải các file pkl nhỏ
        max_wait = 300
        elapsed = 0
        while not os.path.exists(TrainingConfig.EVAL_PKL_PATH) and elapsed < max_wait:
            time.sleep(5)
            elapsed += 5

    if TrainingConfig.RANK == 0:
        print("\n" + "="*60)
        print(f"   AMAZON x VN - SUPER-FAST DISTRIBUTED TRAINING (PRECOMPUTED)")
        print(f"   World Size: {TrainingConfig.WORLD_SIZE} | Mode: {args.baseline}")
        print("="*60 + "\n")

    # Thay thế dist.barrier() cứng nhắc bằng vòng lặp kiểm tra file (Tránh NCCL Timeout)
    # CHỈ ĐỢI NẾU KHÔNG PHẢI LÀ BASELINE 3, 4 HOẶC ALL
    if str(args.baseline) not in ["3", "4", "all"] and TrainingConfig.RANK != 0:
        import time
        logger.info(f"Rank {TrainingConfig.RANK} đang đợi dữ liệu được đồng bộ từ Rank 0...")
        max_wait = 3600  # Đợi tối đa 1 tiếng
        elapsed = 0
        while not os.path.exists(TrainingConfig.ITEM_EMBEDDINGS_PATH) and elapsed < max_wait:
            time.sleep(10)
            elapsed += 10
            if elapsed % 60 == 0:
                logger.info(f"  - Vẫn đang đợi... ({elapsed//60} phút)")
        
        if not os.path.exists(TrainingConfig.ITEM_EMBEDDINGS_PATH):
            logger.error("Quá thời gian chờ tải dữ liệu (Timeout).")
            cleanup_distributed(); return
    else:
        # Rank 0 đợi một chút để đảm bảo file system đã flush (quan trọng trên NFS/Cloud)
        import time
        time.sleep(5)

    if args.baseline == "all":
        baselines_to_run = [1, 2, 3, 4, 5, 6]
    else:
        baselines_to_run = [int(args.baseline)]

    for b_id in baselines_to_run:
        if TrainingConfig.RANK == 0: logger.info(f">>> BẮT ĐẦU BASELINE {b_id} <<<")
        try: run_pipeline(b_id)
        except Exception as e: 
            logger.error(f"Thất bại tại Baseline {b_id}: {e}")
            import traceback; traceback.print_exc()
        dist.barrier()

    if TrainingConfig.RANK == 0: logger.info("TOÀN BỘ PIPELINE ĐÃ HOÀN TẤT!")
    cleanup_distributed()

if __name__ == "__main__":
    main()
