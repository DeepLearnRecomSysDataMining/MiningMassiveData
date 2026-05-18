# Báo cáo Distributed Training cho DSSM và GCN trên Vertex AI

## 1. Tổng quan

Dự án huấn luyện hai baseline chính:

- **Baseline 3: DSSM / Two-Tower Retrieval**
- **Baseline 4: Batched GCN / Graph-based Retrieval**

Cả hai được chạy trên Vertex AI Custom Job với **1 VM, 4 GPU T4**, dùng **PyTorch DistributedDataParallel (DDP)**. Mỗi GPU nhận một shard dữ liệu riêng, train song song, đồng bộ gradient sau mỗi batch, sau đó Rank 0 đánh giá và lưu checkpoint/metrics lên GCS.

---

## 2. Distributed Training là gì?

Distributed training chia quá trình huấn luyện model ra nhiều GPU hoặc nhiều máy. Trong dự án của nhóm, em dùng dạng:

```text
Single-node multi-GPU distributed training
```

Tức là:

```text
dùng 1 VM Vertex AI
VM này có 4 GPU T4
4 process PyTorch
mỗi process điều khiển 1 GPU
```

Mỗi GPU có một bản copy model riêng trong VRAM. Tuy nhiên, nhờ DDP, sau mỗi lần `loss.backward()`, gradient được **all-reduce** và tổng hợp giữa các GPU. Sau code `optimizer.step()`, trọng số của model trên GPU 0, 1, 2, 3 được đồng bộ.

---

## 3. Sơ đồ tổng quan hệ thống

![System Overview](T-each-phase-Diagram\Architect_Distributed_Traning.drawio.png)

```mermaid
flowchart TD
    classDef cloud fill:#e3f2fd,stroke:#1565c0,stroke-width:2px,color:#0d47a1
    classDef gpu fill:#ede7f6,stroke:#512da8,stroke-width:2px,color:#311b92
    classDef data fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px,color:#1b5e20
    classDef model fill:#fff3e0,stroke:#ef6c00,stroke-width:2px,color:#e65100
    classDef save fill:#fce4ec,stroke:#ad1457,stroke-width:2px,color:#880e4f

    A[submit_job.sh]:::cloud --> B[Build Docker Image]:::cloud
    B --> C[Push Artifact Registry]:::cloud
    C --> D[Vertex AI Custom Job]:::cloud
    D --> E[Pull Docker Image]:::cloud
    E --> F[torch.distributed.run]:::cloud

    F --> G0[Rank 0 / GPU 0]:::gpu
    F --> G1[Rank 1 / GPU 1]:::gpu
    F --> G2[Rank 2 / GPU 2]:::gpu
    F --> G3[Rank 3 / GPU 3]:::gpu

    G0 --> H0[Load shard data 0]:::data
    G1 --> H1[Load shard data 1]:::data
    G2 --> H2[Load shard data 2]:::data
    G3 --> H3[Load shard data 3]:::data

    H0 --> I0[Train local batches]:::model
    H1 --> I1[Train local batches]:::model
    H2 --> I2[Train local batches]:::model
    H3 --> I3[Train local batches]:::model

    I0 --> J[DDP Gradient All-Reduce]:::model
    I1 --> J
    I2 --> J
    I3 --> J

    J --> K[Weights synchronized]:::model
    K --> L[Barrier sau epoch]:::cloud
    L --> M[Rank 0 Evaluate]:::model
    M --> N[Save metrics.csv]:::save
    M --> O[Save resume.pt + history.json]:::save
    M --> P[Save best.pt nếu tốt hơn]:::save
    N --> Q[GCS models_checkpoints/]:::save
    O --> Q
    P --> Q
    Q --> R[Final barrier]:::cloud
    R --> S[Next epoch]:::model
```

---

## 4. Luồng 4 GPU trong một epoch

![Train per Epoch](T-each-phase-Diagram\GPUs_in_Epoch.drawio.png)

```mermaid
sequenceDiagram
    participant G0 as GPU 0 / Rank 0
    participant G1 as GPU 1 / Rank 1
    participant G2 as GPU 2 / Rank 2
    participant G3 as GPU 3 / Rank 3
    participant DDP as DDP All-Reduce
    participant GCS as GCS

    Note over G0,G3: Mỗi GPU nhận shard dữ liệu riêng
    G0->>G0: Load batch shard 0
    G1->>G1: Load batch shard 1
    G2->>G2: Load batch shard 2
    G3->>G3: Load batch shard 3

    Note over G0,G3: Train song song
    G0->>G0: forward + loss + backward
    G1->>G1: forward + loss + backward
    G2->>G2: forward + loss + backward
    G3->>G3: forward + loss + backward

    Note over DDP: DDP đồng bộ gradient sau backward
    G0->>DDP: gradient
    G1->>DDP: gradient
    G2->>DDP: gradient
    G3->>DDP: gradient
    DDP-->>G0: synchronized gradient
    DDP-->>G1: synchronized gradient
    DDP-->>G2: synchronized gradient
    DDP-->>G3: synchronized gradient

    G0->>G0: optimizer.step()
    G1->>G1: optimizer.step()
    G2->>G2: optimizer.step()
    G3->>G3: optimizer.step()

    Note over G0,G3: Barrier sau khi hết epoch
    G0->>G0: barrier()
    G1->>G0: wait
    G2->>G0: wait
    G3->>G0: wait

    Note over G0,GCS: Chỉ GPU 0 evaluate và lưu
    G0->>G0: evaluate HR@10 / NDCG@10
    G0->>GCS: upload metrics.csv
    G0->>GCS: upload resume.pt + history.json
    G0->>GCS: upload best.pt nếu tốt hơn

    Note over G0,G3: Final barrier, các GPU chờ Rank 0 lưu xong
    G0->>G1: release
    G0->>G2: release
    G0->>G3: release
```

---

## 5. DSSM Distributed Training

DSSM là mô hình two-tower:

```text
Amazon/query embedding -> Amazon tower -> vector 128D
VN/product embedding    -> VN tower     -> vector 128D
```

Sau đó tính similarity giữa query và product.

Trong pipeline mới:

- Mỗi GPU có một bản DSSM riêng.
- Mỗi rank nhận shard interaction riêng.
- Mỗi batch chỉ được đưa lên GPU khi train.
- Negative sampling dùng **semi-hard top-k in-batch negative**.
- DDP đồng bộ gradient sau mỗi batch.
- Rank 0 evaluate toàn bộ eval set.
- Rank 0 lưu `dssm_metrics.csv`, `dssm_resume.pt`, `dssm_history.json`, `dssm_best.pt`.

```mermaid
flowchart LR
    classDef rank fill:#f3e5f5,stroke:#7b1fa2,stroke-width:2px,color:#4a148c
    classDef sync fill:#fff8e1,stroke:#f9a825,stroke-width:2px,color:#f57f17
    classDef save fill:#fce4ec,stroke:#c2185b,stroke-width:2px,color:#880e4f

    A0[GPU0 DSSM copy]:::rank --> B0[Shard 0]
    A1[GPU1 DSSM copy]:::rank --> B1[Shard 1]
    A2[GPU2 DSSM copy]:::rank --> B2[Shard 2]
    A3[GPU3 DSSM copy]:::rank --> B3[Shard 3]

    B0 --> C[Semi-hard negative]
    B1 --> C
    B2 --> C
    B3 --> C

    C --> D[DDP All-Reduce]:::sync
    D --> E[Weights synchronized]:::sync
    E --> F[Barrier]:::sync
    F --> G[Rank 0 Evaluate]:::rank
    G --> H[Save metrics/resume/best]:::save
    H --> I[GCS]:::save
    I --> J[Final barrier]:::sync
```

---

## 6. GCN Distributed Training

GCN dùng class `BatchedGCN` từ `models.py`. Model nhận:

```text
X: (B, N, 768)
```

và trả:

```text
X_out: (B, N, 128)
```

Ở phiên bản mới, mỗi sample không còn graph 2 node nữa. Mỗi graph gồm:

```text
query + positive + nhiều negative candidates
```

Ví dụ với `num_neg = 8`:

```text
N = 1 query + 1 positive + 8 negatives = 10 nodes
```

Điều này giúp GCN có **graph context thật**, vì các node trong cùng graph được kết nối bằng cosine similarity và adjacency threshold.

```mermaid
flowchart TD
    classDef query fill:#e3f2fd,stroke:#1565c0,stroke-width:2px,color:#0d47a1
    classDef pos fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px,color:#1b5e20
    classDef neg fill:#ffebee,stroke:#c62828,stroke-width:2px,color:#b71c1c
    classDef gcn fill:#fff3e0,stroke:#ef6c00,stroke-width:2px,color:#e65100
    classDef loss fill:#f3e5f5,stroke:#6a1b9a,stroke-width:2px,color:#4a148c

    Q[Query node]:::query
    P[Positive node]:::pos
    N1[Negative 1]:::neg
    N2[Negative 2]:::neg
    N3[Negative 3]:::neg
    N8[Negative 8]:::neg

    Q --> X[Graph X]:::gcn
    P --> X
    N1 --> X
    N2 --> X
    N3 --> X
    N8 --> X

    X --> A[Cosine similarity matrix]:::gcn
    A --> B[Adjacency by threshold]:::gcn
    B --> C[GCN message passing]:::gcn
    C --> QO[Anchor output]:::query
    C --> PO[Positive output]:::pos
    C --> NO[Negative candidates]:::neg

    QO --> S[Semi-hard top-k negative]:::loss
    NO --> S
    S --> L[TripletMarginLoss]:::loss
```

---

## 7. Vì sao chỉ GPU 0 evaluate?

Trong DDP, GPU 0, 1, 2, 3 đều có model copy riêng nhưng gradient được đồng bộ sau mỗi backward. Vì vậy sau mỗi optimizer step:

```text
model GPU0 ≈ model GPU1 ≈ model GPU2 ≈ model GPU3
```

Do đó, evaluate model trên GPU 0 là đủ. Nếu cả 4 GPU cùng evaluate toàn bộ eval set thì vừa tốn tài nguyên, vừa dễ bị tính trùng metrics.

---

## 8. Vì sao cần barrier?

Có hai barrier chính:

### Barrier sau train epoch

Đảm bảo tất cả GPU train xong epoch hiện tại trước khi Rank 0 evaluate.

### Barrier sau evaluate/save

Đảm bảo Rank 0 evaluate và upload checkpoint/metrics xong trước khi các GPU bước sang epoch tiếp theo.

đảm bảo được vì khi dùng Spot VM thì khi bị thu hồi, GCS vẫn có checkpoint gần nhất + history nên lần chạy sau sẽ bắt đầu từ epoch đang dở và train lại từ epoch đó với model đã lưu trọng số từ epoch trước.

---

## 9. Checkpoint và resume khi dùng Spot

Sau mỗi epoch, Rank 0 lưu:

| File | Ý nghĩa |
|---|---|
| `*_resume.pt` | model state, optimizer state, epoch gần nhất, best metric |
| `*_history.json` | lịch sử metric đã train |
| `*_metrics.csv` | bảng metric tổng hợp |
| `*_best.pt` | model tốt nhất theo HR@10 |

Khi job chạy lại:

```text
Rank 0 download resume checkpoint
barrier để các rank chờ
tất cả rank load cùng checkpoint
start_epoch = checkpoint_epoch + 1
train tiếp epoch kế tiếp
```

---

## 10. Đồng bộ loss distributed

Loss local của Rank 0 không đại diện toàn bộ 4 GPU. Vì vậy code mới all-reduce:

```text
loss_tensor = [total_loss, total_batches]
all_reduce SUM
global_avg_loss = total_loss_all_gpu / total_batches_all_gpu
```

`global_avg_loss` mới được ghi vào metrics CSV.

