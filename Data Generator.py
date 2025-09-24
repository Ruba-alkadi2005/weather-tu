import os
import time
import random
from datetime import datetime

# أسماء مدن للتجربة
cities = ["Sana'a", "Aden", "Taiz", "Hodeidah", "Ibb"]

# مجلد الحفظ
folder_name = "weather_data"
os.makedirs(folder_name, exist_ok=True)

while True:
    # اسم الملف مع التاريخ والوقت
    filename = f"weather_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    filepath = os.path.join(folder_name, filename)

    # توليد بيانات عشوائية
    city = random.choice(cities)
    temperature = round(random.uniform(15, 40), 1)  # درجة الحرارة
    humidity = random.randint(30, 90)               # الرطوبة %
    wind_speed = round(random.uniform(0.5, 15), 1)  # سرعة الرياح

    data = f"{datetime.now()} | {city} | Temp: {temperature}°C | Humidity: {humidity}% | Wind: {wind_speed} km/h"

    # كتابة البيانات في ملف جديد
    with open(filepath, "w", encoding="utf-8") as file:
        file.write(data + "\n")

    print(f"تم إنشاء الملف: {filename} \n{data}\n")

    # ينتظر 5 ثواني قبل إنشاء ملف جديد
    time.sleep(5)
