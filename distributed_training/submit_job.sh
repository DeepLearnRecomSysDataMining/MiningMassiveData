# submit_job.sh
PROJECT_ID="your-project-id"
REGION="us-central1"
BUCKET="mining-data-2"
IMAGE_URI="gcr.io/$PROJECT_ID/distributed-training:v1"

# 1. Build & Push Docker image
docker build -t $IMAGE_URI .
docker push $IMAGE_URI

# 2. Gửi job lên Vertex AI (Chạy trên 2 Node, mỗi Node 4 GPU T4)
gcloud ai custom-jobs create \
    --region=$REGION \
    --display-name="RecSys-Distributed-Training" \
    --worker-pool-spec=machine-type=n1-standard-8,replica-count=2,container-image-uri=$IMAGE_URI,accelerator-type=NVIDIA_TESLA_T4,accelerator-count=4 \
    --args="--baseline=all"
