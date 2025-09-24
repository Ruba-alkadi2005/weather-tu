import csv
import os
from kafka import KafkaConsumer

csv_file = r"C:\Users\hp\Desktop\project\weather_received.csv"

# إنشاء الملف إذا ما موجود وإضافة الأعمدة
if not os.path.exists(csv_file):
    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "city", "temperature", "humidity", "wind_speed"])

consumer = KafkaConsumer(
    "weather-topic",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    group_id="weather-group",
    value_deserializer=lambda v: v.decode("utf-8")
)

print("📥 بانتظار الرسائل من Kafka وحفظها في CSV...\n")

for message in consumer:
    data = message.value
    print(f"🌤️ استلمت: {data}")

    # تقسيم البيانات بشكل صحيح
    parts = data.split(" | ")
    if len(parts) == 5:
        timestamp = parts[0].strip()
        city = parts[1].strip()
        temp = parts[2].replace("Temp:", "").replace("°C", "").strip()
        humidity = parts[3].replace("Humidity:", "").replace("%", "").strip()
        wind = parts[4].replace("Wind:", "").replace("km/h", "").strip()

        # كتابة البيانات في CSV
        with open(csv_file, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([timestamp, city, temp, humidity, wind])
