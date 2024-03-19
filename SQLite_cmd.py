import sqlite3

class SQLite_cmd :
  '''
  class Summary_sqlite mengambil data dari database sqlite
  '''
  def __init__(self):
    # Nama database SQLite
    self.sqlite_db = 'Stock_Summary.db'

  # Fungsi untuk membuat tabel di SQLite berdasarkan struktur CSV
  def create_table(self, cursor, table_name:str, columns:list):
      columns_def = ', '.join([f'"{col}" TEXT' for col in columns])
      cursor.execute(f'CREATE TABLE IF NOT EXISTS "{table_name}" ({columns_def})')
      return self
  
  # Fungsi untuk memasukkan data dari CSV ke SQLite
  def insert_data(self, cursor, table_name:str, columns:list, data:list):
      placeholders = ', '.join(['?' for _ in columns])
      cursor.executemany(f'INSERT INTO "{table_name}" VALUES ({placeholders})', data)
      return self

  def delete_table(self,conn, table_name:str):
    cursor = conn.cursor()
    try:
        # Eksekusi perintah SQL untuk menghapus tabel
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        conn.commit()
        print(f"Tabel {table_name} telah dihapus.")
    except sqlite3.Error as e:
        print("Error:", e)
    return self
  
  def delete_column(self, conn, table_name:str, column_name:str):
    cursor = conn.cursor()
    # Definisikan perintah SQL untuk menghapus kolom
    alter_query = f"""
    ALTER TABLE "{table_name}"
    DROP COLUMN "{column_name}"
    """
    
    try:
        # Eksekusi perintah SQL
        cursor.execute(alter_query)
        conn.commit()
        print(f"Kolom {column_name} telah dihapus.")
    except sqlite3.Error as e:
        print("Error:", e)
    return self

  def select_top(conn, table,  n):
    """
    Query n first rows of the table
    parameter :
      conn: the Connection object
      table: The table to query
      n: Number of rows to query
    return :
      rows
    """
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM [{table}] LIMIT :limitNum", {"limitNum": n})

    rows = cur.fetchall()
    return rows