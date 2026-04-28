from kafka import KafkaProducer
import json
import time

# Khởi tạo Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = 'test_bigdata_topic'

print("🚀 Bắt đầu gửi dữ liệu giả lập vào Kafka...")

# Tạo vòng lặp gửi 10 dòng data
for i in range(1, 11):
    data = {
        "id": i,
        "product": f"Sản phẩm test số {i}",
        "rating": 5
    }
    # Bắn vào Kafka
    producer.send(topic_name, value=data)
    print(f"Đã gửi: {data}")
    time.sleep(1) # Tạm dừng 1 giây mỗi lần gửi để dễ quan sát

producer.flush()
print("✅ Hoàn tất gửi data!")