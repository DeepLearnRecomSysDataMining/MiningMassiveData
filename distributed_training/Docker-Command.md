Xem Docker chiếm bao nhiêu
```bash
   docker system df
```

Check Dung lượng Disk VM:
```bash
   df -h
```

Xóa toàn bộ Docker Container đã dừng trên máy:
```bash
   docker container prune -f
```

Xóa toàn bộ image không dùng
```bash
   docker image prune -af
```

Xóa build cache Docker
```bash
   docker builder prune -af
```

Nếu muốn xóa cả volume không dùng
```bash
   docker volume prune -f
```

Xóa toàn bộ mọi thứ Docker (mạnh nhất)

Nếu bạn muốn reset sạch Docker:
```bash
   docker system prune -af --volumes
```

Kiểm tra thư mục Docker thật sự
```bash
   sudo du -sh /var/lib/docker
```
Sau khi prune xong, dung lượng disk sẽ giảm mạnh.

Nếu vẫn đầy disk
Tìm thư mục lớn:
```bash
   sudo du -h --max-depth=1 / | sort -hr
```
và:
```bash
   sudo du -h --max-depth=1 /home | sort -hr
```

Test đúng bằng cách override entrypoint:
```bash
   docker run --rm --entrypoint python test -c "import torch; print(torch.__version__); print(torch.cuda.is_available())"
```

Test thư viện:
```bash
   docker run --rm --entrypoint python test -c "import pandas, pyarrow, gcsfs, transformers, datasets, sentence_transformers; print('ALL OK')"
```

Test chạy main.py thật:
```bash
   docker run --rm test --baseline=1 --skip-download
```

Test gsutil xem có chưa
```bash
   docker run --rm --entrypoint gsutil test version
```



