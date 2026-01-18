import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager

class SupabaseDB:
    def __init__(self, db_url):
        self.db_url = db_url
        self.conn = None

    def connect(self):
        if not self.conn or self.conn.closed:
            self.conn = psycopg2.connect(self.db_url, cursor_factory=RealDictCursor)

    @contextmanager
    def get_cursor(self):
        self.connect()
        cur = self.conn.cursor()
        try:
            yield cur
        finally:
            cur.close()

    def fetch_all(self, query, params=None):
        with self.get_cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall()

    def run_transaction(self, query, params=None):
        try:
            with self.get_cursor() as cur:
                cur.execute(query, params)
                self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            raise e

if __name__ == "__main__":
    client = SupabaseDB("postgresql://postgres:Propane_02_ol@db.yegxgcgbrnrebourxynd.supabase.co:5432/postgres")
    r = client.fetch_all("""SELECT schema_name 
FROM information_schema.schemata 
WHERE schema_name NOT IN ('information_schema', 'pg_catalog') 
AND schema_name NOT LIKE 'pg_toast%'
AND schema_name NOT LIKE 'pg_temp%'
ORDER BY schema_name;""")
    print(r)