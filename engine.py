import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sqlalchemy import create_engine
import re
import time

class RecommendationEngine:
    def __init__(self, db_url):
        self.db_url = db_url
        self.products_df = None
        self.cosine_sim = None
        self.indices = None
        self.load_and_process_data()

    def _clean_text(self, text):
        if not isinstance(text, str):
            return ''
        text = text.lower()
        text = re.sub(r'<.*?>', '', text)
        text = re.sub(r'[^a-z0-9\s]', '', text)
        return text

    def load_and_process_data(self):
        """Memuat ulang semua data dari database dan melakukan pra-kalkulasi."""
        print(f"[{time.ctime()}] Memuat data baru dari database...")
        engine = create_engine(self.db_url)

        query = """
            SELECT
                p.id, p.title, p.deskripsi, p.detail,
                COALESCE(GROUP_CONCAT(t.nama SEPARATOR ' '), '') AS tags
            FROM produks AS p
            LEFT JOIN produk_tag AS pt ON p.id = pt.produk_id
            LEFT JOIN tags AS t ON pt.tag_id = t.id
            GROUP BY p.id, p.title, p.deskripsi, p.detail
        """
        self.products_df = pd.read_sql(query, engine, index_col='id')
        print(f"Engine: Berhasil memuat {len(self.products_df)} produk dari MySQL.")

        self.products_df['content_soup'] = (self.products_df['title'].fillna('').apply(self._clean_text) + ' ') * 3 \
                                         + (self.products_df['tags'].fillna('').apply(self._clean_text) + ' ') * 3 \
                                         + self.products_df['deskripsi'].fillna('').apply(self._clean_text) + ' ' \
                                         + self.products_df['detail'].fillna('').apply(self._clean_text)

        tfidf = TfidfVectorizer(stop_words='english')
        tfidf_matrix = tfidf.fit_transform(self.products_df['content_soup'])
        self.cosine_sim = cosine_similarity(tfidf_matrix, tfidf_matrix)
        self.indices = pd.Series(range(len(self.products_df)), index=self.products_df.index).drop_duplicates()

        print(f"[{time.ctime()}] SELESAI: Pra-kalkulasi kemiripan selesai. Engine siap.")

    def get_recommendations(self, product_id, n_recommendations=5):
        """Memberikan rekomendasi, dengan pengecekan refresh data terlebih dahulu."""
        
        try:
            idx = self.indices[product_id]
        except KeyError:
            return []

        sim_scores = list(enumerate(self.cosine_sim[idx]))
        sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)
        sim_scores = sim_scores[1:n_recommendations+1]
        product_indices = [i[0] for i in sim_scores]
        return self.products_df.index[product_indices].tolist()

