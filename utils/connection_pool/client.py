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

    def get_schema_details(self):
        query = """
        SELECT schema_name 
        FROM information_schema.schemata 
        WHERE schema_name NOT IN ('information_schema', 'pg_catalog') 
        AND schema_name NOT LIKE 'pg_toast%'
        AND schema_name NOT LIKE 'pg_temp%'
        ORDER BY schema_name;
        """
        return self.fetch_all(query)

    def get_table_details(self, schema_name=None):
        query = """
        SELECT table_schema, table_name, table_type
        FROM information_schema.tables
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        """
        params = []
        if schema_name:
            query += " AND table_schema = %s"
            params.append(schema_name)
        query += " ORDER BY table_schema, table_name;"
        return self.fetch_all(query, params)

    def get_all_column_details(self, table_name=None, schema_name=None):
        query = """
        SELECT table_schema, table_name, column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        """
        params = []
        if schema_name:
            query += " AND table_schema = %s"
            params.append(schema_name)
        if table_name:
            query += " AND table_name = %s"
            params.append(table_name)
        query += " ORDER BY table_schema, table_name, ordinal_position;"
        return self.fetch_all(query, params)

    def get_table_preview(self, table_name, schema_name='public', limit=10):
        query = f"SELECT * FROM {schema_name}.{table_name} LIMIT %s;"
        return self.fetch_all(query, (limit,))

    def fetch(self, query, params=None):
        return self.fetch_all(query, params)

    def fetch_one(self, query, params=None):
        with self.get_cursor() as cur:
            cur.execute(query, params)
            result = cur.fetchone()
            return dict(result) if result else None

    def fetch_all(self, query, params=None):
        with self.get_cursor() as cur:
            cur.execute(query, params)
            return [dict(row) for row in cur.fetchall()]


    # def run_transaction(self, query, params=None):
    #     try:
    #         with self.get_cursor() as cur:
    #             cur.execute(query, params)
    #             self.conn.commit()
    #     except Exception as e:
    #         self.conn.rollback()
    #         raise e

if __name__ == "__main__":
    client = SupabaseDB("postgresql://postgres:Propane_02_ol@db.yegxgcgbrnrebourxynd.supabase.co:5432/postgres")
    # r = client.get_schema_details()
    # print("Schemas:", r)
    
    # Test fetch_one
    # result = client.fetch_all("SELECT schema_name FROM information_schema.schemata")
    # print("Fetch one result:", result)

    