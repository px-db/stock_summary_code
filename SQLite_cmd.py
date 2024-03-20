import sqlite3

class SQLite_cmd :
  '''
  class Summary_sqlite mengambil data dari database sqlite
  '''
  def __init__(self, sqlite_db:str='Stock_Summary.sqlite'):
    self.set_conn(sqlite_db)

  def set_conn(self,sqlite_db:str):
    self.sqlite_db = sqlite_db
    try:
      self.conn = sqlite3.connect(self.sqlite_db)
      print(f"{sqlite_db} has conneted.")
    except sqlite3.Error as e:
      self.print_e(e)
    return self

  # Fungsi untuk membuat tabel di SQLite berdasarkan struktur CSV
  def create_table(self, table_name:str, columns:list):
      cursor = self.conn.cursor()
      columns_def = ', '.join([f'"{col}" TEXT' for col in columns])
      cursor.execute(f'CREATE TABLE IF NOT EXISTS "{table_name}" ({columns_def})')
      return self
  
  # Fungsi untuk memasukkan data dari CSV ke SQLite
  def insert_data(self, table_name:str, columns:list, data:list):
      cursor = self.conn.cursor()
      placeholders = ', '.join(['?' for _ in columns])
      cursor.executemany(f'INSERT INTO "{table_name}" VALUES ({placeholders})', data)
      return self

  def delete_table(self,table_name:str):
    cursor = self.conn.cursor()
    try:
      # Eksekusi perintah SQL untuk menghapus tabel
      cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
      self.conn.commit()
      print(f"Table {table_name} has deleted.")
    except sqlite3.Error as e:
      self.print_e(e)
    return self
  
  def delete_column(self, table_name:str, column_name:str):
    cursor = self.conn.cursor()
    # Definisikan perintah SQL untuk menghapus kolom
    alter_query = f"""
    ALTER TABLE "{table_name}"
    DROP COLUMN "{column_name}"
    """    
    try:
        # Eksekusi perintah SQL
        cursor.execute(alter_query)
        self.conn.commit()
        print(f"column {column_name} has deleted.")
    except sqlite3.Error as e:
        self.print_e(e)
    return self
  
  def delete_row(self,table_name:str, condition=None):
    '''
    sebelum delete apstikan cek data lewat metode
    self.select_row(self,table_name, column, value)

    parameter :
      table_name:str
      condition:str  = "column = value"
    '''
    cursor = self.conn.cursor()
    try:
      # Eksekusi perintah SQL untuk menghapus baris
      if condition == None :
        # Delete all
        cursor.execute(f"DELETE FROM {table_name}")
        self.conn.commit()
        print("all rows has deleted.")
      else :
        # Delete condition
        cursor.execute(f"DELETE FROM {table_name} WHERE {condition}")
        self.conn.commit()
        print("rows has deleted.")
    except sqlite3.Error as e:
      self.print_e(e)
    return self

  def select_row(self,table_name, column, value):
    cursor = self.conn.cursor()
    try:
      # Eksekusi perintah SQL untuk memilih satu baris dengan kondisi tertentu
      cursor.execute(f"SELECT * FROM {table_name} WHERE {column} = ?", (value,))      
      # Ambil satu baris yang dipilih
      row = cursor.fetchone()    
    except sqlite3.Error as e:
      self.print_e(e)
    return row

  def select_top(self, table,  n):
    """
    Query n first rows of the table
    parameter :
      conn: the Connection object
      table: The table to query
      n: Number of rows to query
    return :
      rows
    """
    cur = self.conn.cursor()
    cur.execute(f"SELECT * FROM [{table}] LIMIT :limitNum", {"limitNum": n})
    rows = cur.fetchall()
    return rows
  
  def select(self, table, columns=None):
    cur = self.conn.cursor()
    if columns == None :
      cur.execute(f"SELECT * FROM [{table}]")
    else :
      columns_def = ', '.join([f'"{col}" TEXT' for col in columns])
      cur.execute(f"SELECT ({columns_def}) FROM [{table}]")

    rows = cur.fetchall()
    return rows
  
  def print_e(self,e):
     print("Error:", e)
     return self
  
  def close_conn(self):
     self.conn.close()
     print(f'{self.sqlite_db} has closed')
     return self