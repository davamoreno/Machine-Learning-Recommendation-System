import os
import threading # <-- 1. Import library untuk background thread
import redis
import time
from flask import Flask, request, jsonify
from dotenv import load_dotenv
from engine import RecommendationEngine

# --- Setup Aplikasi ---
load_dotenv()
app = Flask(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise Exception("DATABASE_URL tidak ditemukan di file .env")

# --- BUAT SATU "OTAK" UTAMA YANG AKAN DIPAKAI BERSAMA ---
# Ini adalah satu-satunya instance RecommendationEngine yang ada di aplikasi kita.
recommendation_engine = RecommendationEngine(db_url=DATABASE_URL)


# --- LOGIKA "PENDENGAR" YANG AKAN BERJALAN DI LATAR BELAKANG ---
def redis_listener():
    """Fungsi ini akan dijalankan di dalam sebuah background thread."""
    print("THREAD PENDENGAR: Dimulai...")
    r = redis.Redis(decode_responses=True)
    pubsub = r.pubsub()
    pubsub.subscribe('recommendation-updates')
    print("THREAD PENDENGAR: Siap mendengarkan notifikasi di channel 'recommendation-updates'...")

    for message in pubsub.listen():
        if message['type'] == 'subscribe':
            continue

        if message['data'] == 'refresh':
            print(f"[{time.ctime()}] THREAD PENDENGAR: Menerima sinyal refresh dari Laravel!")
            try:
                # PENTING: Ini memperbarui "Otak" utama yang dipakai bersama.
                recommendation_engine.load_and_process_data()
                print(f"[{time.ctime()}] THREAD PENDENGAR: Engine berhasil di-refresh.")
            except Exception as e:
                print(f"Error saat me-refresh engine: {e}")


# --- MULAI THREAD PENDENGAR SAAT APLIKASI DIJALANKAN ---
# daemon=True memastikan thread ini akan mati saat aplikasi utama dimatikan.
listener_thread = threading.Thread(target=redis_listener, daemon=True)
listener_thread.start()


# --- ENDPOINT API (PINTU DEPAN UNTUK LARAVEL) ---
@app.route('/recommend', methods=['POST'])
def recommend():
    # Endpoint ini sekarang menggunakan instance engine yang sama dengan yang diperbarui oleh thread.
    data = request.get_json()
    if not data or 'product_id' not in data:
        return jsonify({"error": "Mohon sediakan 'product_id'."}), 400

    try:
        product_id = int(data['product_id'])
        recommendations = recommendation_engine.get_recommendations(product_id)
        return jsonify({"recommendations": recommendations})
    except Exception as e:
        return jsonify({"error": "Terjadi kesalahan internal.", "details": str(e)}), 500


# (Perintah if __name__ == '__main__': tidak lagi diperlukan jika menggunakan 'flask run')
