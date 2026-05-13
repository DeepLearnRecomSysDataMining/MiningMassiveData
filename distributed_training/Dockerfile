FROM pytorch/pytorch:2.4.0-cuda12.4-cudnn9-runtime

WORKDIR /app

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ /app/src/
COPY config/ /app/config/
COPY data_loader/ /app/data_loader/
COPY main.py /app/

ENV PYTHONPATH="/app"
ENV TRAINING_ENV="cloud"

ENTRYPOINT ["python", "-m", "torch.distributed.run", "--nproc_per_node=auto", "main.py"]
