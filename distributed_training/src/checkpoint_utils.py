import os
import json
import torch
import logging
import subprocess

from config.training_config import TrainingConfig

logger = logging.getLogger("checkpoint_utils")

def upload_file_to_gcs(local_path: str):
    if TrainingConfig.RANK != 0:
        return

    if not os.path.exists(local_path):
        logger.warning(f"Skip upload, file not found: {local_path}")
        return

    gcs_path = f"{TrainingConfig.GCS_MODEL_PATH}/{os.path.basename(local_path)}"
    subprocess.run(["gsutil", "cp", local_path, gcs_path], check=False)
    logger.info(f"Uploaded {local_path} -> {gcs_path}")

def _unwrap_model(model):
    return model.module if hasattr(model, "module") else model


def get_ckpt_paths(model_name: str):
    local_path = os.path.join( TrainingConfig.LOCAL_MODELS_DIR, f"{model_name}_resume.pt" )

    local_history_path = os.path.join( TrainingConfig.LOCAL_MODELS_DIR, f"{model_name}_history.json" )

    gcs_path = f"{TrainingConfig.GCS_MODEL_PATH}/{model_name}_resume.pt"
    gcs_history_path = f"{TrainingConfig.GCS_MODEL_PATH}/{model_name}_history.json"

    return local_path, local_history_path, gcs_path, gcs_history_path


def download_resume_checkpoint(model_name: str):
    local_path, local_history_path, gcs_path, gcs_history_path = get_ckpt_paths(model_name)

    os.makedirs(TrainingConfig.LOCAL_MODELS_DIR, exist_ok=True)

    subprocess.run(["gsutil", "cp", gcs_path, local_path], check=False)
    subprocess.run(["gsutil", "cp", gcs_history_path, local_history_path], check=False)

    return local_path, local_history_path


def load_resume_checkpoint(model_name: str, model, optimizer=None, device="cuda"):
    local_path, local_history_path, _, _ = get_ckpt_paths(model_name)

    if not os.path.exists(local_path):
        logger.info(f"No resume checkpoint found for {model_name}. Start from scratch.")
        return {
            "start_epoch": 0,
            "best_metric": 0.0,
            "history": []
        }

    try:
        ckpt = torch.load(local_path, map_location=device)
    except Exception as e:
        logger.warning(f"Resume checkpoint invalid/corrupted for {model_name}: {e}. Start from scratch.")
        return {
            "start_epoch": 0,
            "best_metric": 0.0,
            "history": []
        }

    _unwrap_model(model).load_state_dict(ckpt["model_state"])

    if optimizer is not None and "optimizer_state" in ckpt:
        optimizer.load_state_dict(ckpt["optimizer_state"])

    history = ckpt.get("history", [])

    logger.info(
        f"Loaded {model_name} checkpoint: "
        f"epoch={ckpt.get('epoch', -1)}, best_metric={ckpt.get('best_metric', 0.0)}"
    )

    return {
        "start_epoch": int(ckpt.get("epoch", -1)) + 1,
        "best_metric": float(ckpt.get("best_metric", 0.0)),
        "history": history
    }


def save_resume_checkpoint( model_name: str, model, optimizer, epoch: int, best_metric: float, history: list):
    local_path, local_history_path, gcs_path, gcs_history_path = get_ckpt_paths(model_name)

    os.makedirs(TrainingConfig.LOCAL_MODELS_DIR, exist_ok=True)

    ckpt = {
        "model_name": model_name,
        "epoch": epoch,
        "best_metric": best_metric,
        "model_state": _unwrap_model(model).state_dict(),
        "optimizer_state": optimizer.state_dict(),
        "history": history,
    }

    torch.save(ckpt, local_path)

    with open(local_history_path, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)

    if TrainingConfig.RANK == 0:
        subprocess.run(["gsutil", "cp", local_path, gcs_path], check=False)
        subprocess.run(["gsutil", "cp", local_history_path, gcs_history_path], check=False)

    logger.info(f"Saved resume checkpoint for {model_name} at epoch {epoch}.")

def save_best_model( model_name: str, model, epoch: int, metrics: dict):
    local_best_path = os.path.join( TrainingConfig.LOCAL_MODELS_DIR, f"{model_name}_best.pt" )
    gcs_best_path = f"{TrainingConfig.GCS_MODEL_PATH}/{model_name}_best.pt"

    os.makedirs(TrainingConfig.LOCAL_MODELS_DIR, exist_ok=True)

    torch.save(
        {
            "model_name": model_name,
            "epoch": epoch,
            "metrics": metrics,
            "model_state": _unwrap_model(model).state_dict()
        },
        local_best_path
    )

    if TrainingConfig.RANK == 0:
        subprocess.run(["gsutil", "cp", local_best_path, gcs_best_path], check=False)

    logger.info(f"Saved BEST model for {model_name} at epoch {epoch}")