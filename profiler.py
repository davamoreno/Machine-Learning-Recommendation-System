import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
import time

def build_and_save_recommendation():
    """Fungsi ini akan membangun model rekomendasi dan menyimpannya ke database."""
    load_dotenv()
    db_url = os.getenv("DATABASE_URL")
    engine = create_engine(db_url)
    
    print(f"[{time.ctime()}] Memulai proses rekomendasi...")
    
    views = pd.read_sql("SELECT user_id, produk_id FROM user_product_views", engine)
    
    if views.empty:
        print("PROFILER: Tidak ada data view baru untuk diproses. Selesai.")
        return
    produk_features = pd.read_sql("""
                                    SELECT p.id as produk_id, p.kategori_id,
                                    t.id as tag_id FROM produks p
                                    LEFT JOIN produk_tag pt ON p.id = pt.produk_id
                                    LEFT JOIN tags t ON pt.tag_id = t.id
                                """, engine)
 
    interactions_features = pd.merge(views, produk_features, on='produk_id')
    kategori_profile = interactions_features.groupby(['user_id', 'kategori_id']).size().reset_index(name='score')
    tag_profile = interactions_features.groupby(['user_id', 'tag_id']).size().reset_index(name='score')
    
    user_profiles = {}
    
    for _, row in kategori_profile.iterrows():
        user_id, feature_id, score = row['user_id'], row['kategori_id'], row['score']
        user_profiles.setdefault(user_id, {})[f"kategori_{feature_id}"] = score
    for _, row in tag_profile.iterrows():
        user_id, feature_id, score = row['user_id'], row['tag_id'], row['score']
        user_profiles.setdefault(user_id, {})[f"tag_{feature_id}"] = score
    
    all_reccommendations = []
    viewed_items = views.groupby('user_id')['produk_id'].apply(set).to_dict()
    
    for user_id, profile in user_profiles.items():
        user_viewed = viewed_items.get(user_id, set())
        products_to_score = produk_features[~produk_features['produk_id'].isin(user_viewed).copy()]
        
        def calculate_score(row):
            score = 0
            if pd.notna(row['kategori_id']):
                score += profile.get(f'kategori_{row["kategori_id"]}', 0)
            if pd.notna(row['tag_id']):
                score += profile.get(f'tag_{row["tag_id"]}', 0)
            return score
        
        products_to_score['score'] = products_to_score.apply(calculate_score, axis=1)
        
        top_recs = products_to_score.groupby('produk_id')['score'].sum().nlargest(20).reset_index()
        top_recs['user_id'] = user_id
        all_reccommendations.append(top_recs)
        
    if all_reccommendations:
        final_recs_df = pd.concat(all_reccommendations, ignore_index=True)
        final_recs_df['created_at'] = pd.to_datetime('now')
        final_recs_df['updated_at'] = pd.to_datetime('now')
        
        with engine.connect() as conn:
            conn.execute(text("TRUNCATE TABLE user_dashboard_recommendations"))
            conn.commit()
        
        final_recs_df[['user_id', 'produk_id', 'score', 'created_at', 'updated_at']].to_sql('user_dashboard_recommendations', engine, if_exists='append', index=False)
        
        print(f"[{time.ctime()}] Rekomendasi berhasil disimpan ke database.")
    else:
        print(f"[{time.ctime()}] Tidak ada rekomendasi yang dibuat, tidak ada data yang disimpan.")
        
if __name__ == "__main__":
    build_and_save_recommendation()
    print("PROFILER: Proses rekomendasi selesai.")