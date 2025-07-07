import redis
import time
import os
from dotenv import load_dotenv
from engine import RecommendationEngine # Kita akan pakai engine yang sama

# Muat konfigurasi dari .env
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

# --- Logika Worker ---
def main():
    print("Worker rekomendasi dimulai...")
    # Hubungkan ke Redis (defaultnya ke localhost:6379)
    r = redis.Redis(decode_responses=True)
    # Berlangganan ke 'saluran radio' kita
    pubsub = r.pubsub()
    pubsub.subscribe('recommendation-updates')

    print("Mendengarkan notifikasi dari Laravel di channel 'recommendation-updates'...")
    
    # Buat instance engine sekali saja
    engine = RecommendationEngine(db_url=DATABASE_URL)

    # Loop selamanya untuk mendengarkan pesan
    for message in pubsub.listen():
        # Abaikan pesan pertama saat subscribe
        if message['type'] == 'subscribe':
            continue

        # Jika ada pesan 'refresh' masuk dari Laravel...
        if message['data'] == 'refresh':
            print(f"[{time.ctime()}] Menerima sinyal refresh dari Laravel!")
            
            # ...maka kita panggil ulang method untuk memuat ulang data di engine.
            try:
                engine.load_and_process_data()
                print(f"[{time.ctime()}] Engine berhasil di-refresh dengan data terbaru.")
            except Exception as e:
                print(f"Error saat me-refresh engine: {e}")

if __name__ == '__main__':
    main()
