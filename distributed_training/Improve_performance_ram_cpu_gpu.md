Đề xuất sửa theo 3 mức, ưu tiên **ổn định + giảm CPU/RAM** trước.

## 1. Sửa ngay trong code hiện tại

### DataLoader

Bạn đang để `num_workers=2`, 4 GPU là 8 workers. Nếu CPU/RAM còn cao, đổi về `1`:

```python
loader = DataLoader(
    train_set,
    batch_size=TrainingConfig.BATCH_SIZE,
    sampler=sampler,
    num_workers=1,
    pin_memory=True,
    persistent_workers=True,
    prefetch_factor=2
)
```

Nếu `num_workers=0` thì bỏ `persistent_workers` và `prefetch_factor`.

### Optimizer

Đổi:

```python
optimizer.zero_grad()
```

thành:

```python
optimizer.zero_grad(set_to_none=True)
```

nhẹ RAM hơn một chút.

### Target tensor

Đổi:

```python
torch.ones_like(pos_score).to(device)
```

thành:

```python
target = torch.ones_like(pos_score)
loss = criterion(pos_score, neg_score, target)
```

vì `pos_score` đã ở `device`.

---

## 2. Batch size / learning rate

Với `BATCH_SIZE=2048`, DSSM có thể chạy được. Tôi không tăng batch ngay.

Giữ:

```python
BATCH_SIZE = 2048
LR = 1e-3
```

Nếu thấy HR/NDCG không tăng sau 1–2 epoch, giảm LR:

```python
LR = 5e-4
```

Nếu GPU utilization quá thấp nhưng CPU vẫn chịu được, thử:

```python
BATCH_SIZE = 4096
LR = 1e-3
```

Không nên vừa tăng batch vừa tăng LR ngay.

---

## 3. Điểm nghẽn lớn nhất vẫn là pandas `iloc`

Dòng này tốn CPU nhiều:

```python
row = self.df.iloc[int(idx)]
```

Tối ưu tốt hơn là trong `__init__` đổi DataFrame thành numpy arrays:

```python
self.asins = interactions_df["asin"].astype(str).to_numpy()
self.product_ids = interactions_df["product_id"].astype(str).to_numpy()
```

rồi `__getitem__`:

```python
asin = self.asins[int(idx)]
product_id = self.product_ids[int(idx)]
```

Nhanh hơn `iloc` khá nhiều.

Code sửa:

```python
class DSSMTrainingDataset(Dataset):
    def __init__(self, interactions_df, embedding_lookup):
        self.asins = interactions_df["asin"].astype(str).to_numpy()
        self.product_ids = interactions_df["product_id"].astype(str).to_numpy()
        self.lookup = embedding_lookup

    def __len__(self):
        return len(self.asins)

    def __getitem__(self, idx):
        idx = int(idx)
        q_emb = self.lookup.get_embedding(f"amz_{self.asins[idx]}")
        p_emb = self.lookup.get_embedding(f"vn_{self.product_ids[idx]}")
        return torch.from_numpy(q_emb.copy()).float(), torch.from_numpy(p_emb.copy()).float()
```

Đây là sửa đáng làm nhất để giảm tải CPU mà không ảnh hưởng chất lượng training.
