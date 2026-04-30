#!/bin/bash

# ============================================================
# CENTRAL ORCHESTRATION SCRIPT - GCP RECSYS PIPELINE
# ============================================================

# --- 0. CẤU HÌNH (Thay đổi các giá trị này) ---
GCS_BUCKET="gs://my-recsys-project-bucket"
DRIVE_FOLDER_ID="YOUR_DRIVE_ID"
DATAPROC_CLUSTER="my-spark-cluster"
REGION="us-central1"
PROJECT_ID=$(gcloud config get-value project)

echo "----------------------------------------------------------"
echo "   STARTING COMPLETE RECSYS PIPELINE ON GCP"
echo "   Project: $PROJECT_ID | Bucket: $GCS_BUCKET"
echo "----------------------------------------------------------"

# --- BƯỚC 1: INGESTION (Drive -> GCS) ---
echo -e "\n[STEP 1] Ingesting data from Google Drive to GCS..."
# Cập nhật cấu hình trong file python trước khi chạy hoặc dùng biến môi trường
export GCS_DESTINATION="$GCS_BUCKET/raw_data/amazon_gpc/"
export GOOGLE_DRIVE_ID="$DRIVE_FOLDER_ID"
python3 download_data.py

if [ $? -ne 0 ]; then echo "Step 1 Failed!"; exit 1; fi

# --- BƯỚC 2: SPARK ETL (GCS Raw -> GCS Processed) ---
echo -e "\n[STEP 2] Submitting PySpark Job to Dataproc Cluster: $DATAPROC_CLUSTER..."
gcloud dataproc jobs submit pyspark spark_processing_gpc/main.py \
    --cluster=$DATAPROC_CLUSTER \
    --region=$REGION \
    --files="spark_processing_gpc/src/file_utils.py,spark_processing_gpc/src/etl_interactions.py,spark_processing_gpc/src/etl_item_nodes.py,spark_processing_gpc/src/schema_scanner.py,spark_processing_gpc/src/data_validator.py,spark_processing_gpc/src/evaluation_dataset.py" \
    --py-files="spark_processing_gpc/src/file_utils.py" \
    --properties="spark.dynamicAllocation.enabled=true" \
    -- \
    --data-dir "$GCS_BUCKET/raw_data/amazon_gpc/"

if [ $? -ne 0 ]; then echo "Step 2 Failed!"; exit 1; fi

# --- BƯỚC 3: DISTRIBUTED TRAINING (GCS Processed -> Models) ---
echo -e "\n[STEP 3] Starting Distributed Training (SBERT Baseline)..."
echo "Note: Buoc nay thuong duoc chay truc tiep tren cac GPU Nodes."
echo "Dang kich hoat train_sbert.py voi torchrun..."

# Gia su ban dang o tren mot trong cac GPU node hoac co moi truong ho tro
export GCS_BUCKET="$GCS_BUCKET"
export SPARK_ENV="cloud"

# Lenh mau de chay tren 1 node co nhieu GPU (DDP)
torchrun --nproc_per_node=auto distributed_training/train_sbert.py \
    --batch_size 16 \
    --epochs 5

echo "----------------------------------------------------------"
echo "   PIPELINE COMPLETED SUCCESSFULLY!"
echo "   Check your models at: $GCS_BUCKET/models/"
echo "----------------------------------------------------------"
