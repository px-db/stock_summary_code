import sqlite3

class SQLite_cmd :
  '''
  class Summary_sqlite mengambil data dari database sqlite
  '''
  def __init__(self, file_sqlite:str='Stock_Summary.sqlite'):
    self.sqlite_db  = file_sqlite
    self.conn       = None
    self.cursor     = None
    self.set_conn(self.sqlite_db)

  # ###############################################################################################
  # OPERASI NON CRUD
  # ###############################################################################################
  def set_conn(self,sqlite_db:str):
    try:
      self.conn = sqlite3.connect(sqlite_db)
      self.cursor = self.conn.cursor()
      print(f"Database {sqlite_db} connected successfully!.")
    except sqlite3.Error as e:
      self.print_e(e)
    return self
  
  def close_conn(self):
     self.conn.close()
     print(f'{self.sqlite_db} has closed')
     return self
  
  def check_conn(self):
    if not self.conn:
      print("No active database connection. Please connect to the database first.")
      print("run .set_conn('db_file')")
      return False
    return True
  
  # Fungsi untuk membuat tabel di SQLite
  def create_table(self, table_name:str, columns:list):
    if not self.check_conn() : return None
    columns_def = ', '.join([f'"{col}" TEXT' for col in columns])
    self.cursor.execute(f'CREATE TABLE IF NOT EXISTS "{table_name}" ({columns_def})')
    return self
  
  # ###############################################################################################
  # CREATE
  # Kelompok Create : INSERT
  # ###############################################################################################

  # Fungsi untuk memasukkan data dari CSV ke SQLite
  def insert_data_from_csv(self, table_name:str, columns:list, data:list):
    '''
    # Baca CSV dari string menggunakan StringIO
      csv_file = StringIO(csv_content)
      reader = csv.reader(csv_file)
      columns = next(reader)  # Ambil nama kolom dari baris pertama
      data = list(reader)     # Ambil data dari sisa baris

    '''
    if not self.check_conn() : return None
    placeholders = ', '.join(['?' for _ in columns])
    self.cursor.executemany(f'INSERT INTO "{table_name}" VALUES ({placeholders})', data)
    return self
  
  def insert_data(self, table:str, data:list):
    """
    query insert data satu baris :
      INSERT INTO nama_tabel (kolom1, kolom2, kolom3, ...)
      VALUES (nilai1, nilai2, nilai3, ...);
      atau
      INSERT INTO nama_tabel
      VALUES (nilai1, nilai2, nilai3, ...);

    query insert data lebih dari 1 :
      INSERT INTO nama_tabel (kolom1, kolom2, kolom3, ...)
      VALUES (nilai1, nilai2, nilai3, ...),
             (nilai1, nilai2, nilai3, ...),
             (nilai1, nilai2, nilai3, ...);
      atau
      INSERT INTO nama_tabel
      VALUES (nilai1, nilai2, nilai3, ...),
             (nilai1, nilai2, nilai3, ...),
             (nilai1, nilai2, nilai3, ...);
    
    parameter :
      table : nama tabel
      data  : (data1, data2, data3, ...)

    """
    if not self.check_conn() : return None
    # Membuat query SQL untuk memasukkan data
    query = f"INSERT INTO {table} VALUES ({', '.join(['?' for _ in range(len(data))])})"
    
    try:
      # Menjalankan query untuk memasukkan data
      self.cursor.execute(query, data)
      self.conn.commit()
      print("Data has been successfully entered into table {table}.")
    except sqlite3.Error as e:
      self.print_e(e)
      self.conn.rollback()
      print("data is returned to original state")
    return self

  # ###############################################################################################
  # READ
  # Kelompok READ : SELECT
  # ###############################################################################################

  def select_row(self,table_name, column, value):
    if not self.check_conn() : return None
    try:
      # Eksekusi perintah SQL untuk memilih satu baris dengan kondisi tertentu
      self.cursor.execute(f"SELECT * FROM {table_name} WHERE {column} = ?", (value,))      
      # Ambil satu baris yang dipilih
      row = self.cursor.fetchone()
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
    self.cursor.execute(f"SELECT * FROM [{table}] LIMIT :limitNum", {"limitNum": n})
    rows = self.cursor.fetchall()
    return rows
  
  def select(self, table, columns=None):
    if not self.check_conn() : return None
    if columns == None :
      self.cursor.execute(f"SELECT * FROM [{table}]")
    else :
      columns_def = ', '.join([f'"{col}" TEXT' for col in columns])
      self.cursor.execute(f"SELECT ({columns_def}) FROM [{table}]")
    rows = self.cursor.fetchall()
    return rows

  # ###############################################################################################
  # UPDATE
  # Kelompok update : UPDATE
  # ###############################################################################################


  def update(self):
    if not self.check_conn() : return None
    pass

  # ###############################################################################################
  # DELETE
  # Kelompok delete : DELETE
  # ###############################################################################################

  def delete_table(self,table_name:str):
    if not self.check_conn() : return None
    try:
      # Eksekusi perintah SQL untuk menghapus tabel
      self.cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
      self.conn.commit()
      print(f"Table {table_name} has deleted.")
    except sqlite3.Error as e:
      self.print_e(e)
      self.conn.rollback()
      print("data is returned to original state")
    return self
  
  def delete_column(self, table_name:str, column_name:str):
    if not self.check_conn() : return None
    # Definisikan perintah SQL untuk menghapus kolom
    alter_query = f"""
    ALTER TABLE "{table_name}"
    DROP COLUMN "{column_name}"
    """    
    try:
      # Eksekusi perintah SQL
      self.cursor.execute(alter_query)
      self.conn.commit()
      print(f"column {column_name} has deleted.")
    except sqlite3.Error as e:
      self.print_e(e)
      self.conn.rollback()
      print("data is returned to original state")
    return self
  
  def delete_row(self,table_name:str, condition=None):
    '''
    sebelum delete apstikan cek data lewat metode
    self.select_row(self,table_name, column, value)

    parameter :
      table_name:str
      condition:str  = "column = value"
    '''
    if not self.check_conn() : return None
    try:
      # Eksekusi perintah SQL untuk menghapus baris
      if condition == None :
        # Delete all
        self.cursor.execute(f"DELETE FROM {table_name}")
        self.conn.commit()
        print("all rows has deleted.")
      else :
        # Delete condition
        self.cursor.execute(f"DELETE FROM {table_name} WHERE {condition}")
        self.conn.commit()
        print("rows has deleted.")
    except sqlite3.Error as e:
      self.print_e(e)
      self.conn.rollback()
      print("data is returned to original state")
    return self

  # ###############################################################################################
  # GENERAL CRUD
  # Untuk query umum
  # ###############################################################################################
  def query_execute(self, query):
    if not self.check_conn() : return None
    self.cursor.execute(query)
    return self.cursor.fetchall()
  
  

  
  

  def print_e(self,e):
     print("Error:", e)
     return self