import os
from kafka import KafkaProducer
import time

# ----- إعداد Producer -----
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: v.encode('utf-8')  # تحويل النص إلى bytes
)

topic_name = "weather-topic"
folder_name = "weather_data"
sent_files = set()  # لتجنب إرسال نفس الملف مرتين

while True:
    # قراءة جميع الملفات في المجلد
    files = sorted(os.listdir(folder_name))
    for file in files:
        if file not in sent_files:
            filepath = os.path.join(folder_name, file)
            with open(filepath, "r", encoding="utf-8") as f:
                data = f.read().strip()
                producer.send(topic_name, value=data)
                print(f"✅ تم إرسال {file} إلى Kafka: {data}")
            sent_files.add(file)
    time.sleep(2)  # التحقق من ملفات جديدة كل ثانيتين
