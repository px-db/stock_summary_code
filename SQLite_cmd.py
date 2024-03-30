import sqlite3
import pandas as pd

class SQLite_cmd :
  '''
  #################################################################################################
  class Summary_sqlite mengambil data dari database sqlite
  #################################################################################################
  '''
  def __init__(self, file_sqlite):
    self.sqlite_db  = file_sqlite
    self.conn       = None
    self.cursor     = None
    self.set_conn(self.sqlite_db)
    self.__table = ''

  # ###############################################################################################
  # OPERASI NON CRUD
  # DDL (Data Definition Language)
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
    ALTER TABLE {table_name}
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
  
  # Fungsi untuk membuat tabel di SQLite
  def create_table(self, table_name:str, columns:list):
    if not self.check_conn() : return None
    columns_def = ', '.join([f'"{col}" TEXT' for col in columns])
    self.cursor.execute(f'CREATE TABLE IF NOT EXISTS {table_name} ({columns_def})')
    return self
  
  def create_table_from_csv(self, table_name, file_csv):
    if not self.check_conn() : return None
    cols = pd.read_csv(file_csv).columns.tolist()
    self.create_table(table_name,cols)
    return self
  
  def csv_to_sqlite(self, table_name, file_csv, cols=None):
    if not self.check_conn() : return None
    self.df_to_sqlite(table_name,
                      pd.read_csv(file_csv,
                                  usecols=cols
                                  )
                      )
    return self
  
  def xlsx_to_sqlite(self, table_name, file_xlsx, cols=None):
    if not self.check_conn() : return None
    self.df_to_sqlite(table_name,
                      pd.read_excel(file_xlsx,
                                    usecols=cols
                                    )
                      )
    return self
  
  def df_to_sqlite(self, table_name, df):
    if not self.check_conn() : return None
    df.to_sql(table_name,
              self.conn,
              if_exists='append',
              index=False)
    return self
  
  def get_column_to_list(self,table_name=None)->list :
    if not self.check_conn() : return None
    if not table_name : table_name = self.__table
    # Mendapatkan nama kolom dari tabel
    self.cursor.execute(f"PRAGMA table_info({table_name})")
    columns = self.cursor.fetchall()    
    # Mengonversi nama kolom menjadi list
    return [column[1] for column in columns]
  
  def add_column(self, table_name:str, column_name:str, data_type:str):
    '''
    ALTER TABLE nama_tabel
    ADD COLUMN nama_kolom tipe_data;

    '''
    alter_query = f"""
    ALTER TABLE {table_name}
    ADD COLUMN "{column_name}" {data_type};
    """    
    try:
      # Eksekusi perintah SQL
      self.cursor.execute(alter_query)
      self.conn.commit()
      print(f"column {column_name} has added.")
    except sqlite3.Error as e:
      self.print_e(e)
      self.conn.rollback()
      print("data is returned to original state")
    return self

  # ###############################################################################################
  # CREATE (INSERT)
  #   INSERT INTO users (name, age) VALUES (?, ?);
  # query insert data satu baris :
  #     INSERT INTO nama_tabel (kolom1, kolom2, kolom3, ...)
  #     VALUES (nilai1, nilai2, nilai3, ...);
  #     atau
  #     INSERT INTO nama_tabel
  #     VALUES (nilai1, nilai2, nilai3, ...);
  # 
  # query insert data lebih dari 1 :
  #   INSERT INTO nama_tabel (kolom1, kolom2, kolom3, ...)
  #   VALUES (nilai1, nilai2, nilai3, ...),
  #          (nilai1, nilai2, nilai3, ...),
  #          (nilai1, nilai2, nilai3, ...);
  #   atau
  #   INSERT INTO nama_tabel
  #   VALUES (nilai1, nilai2, nilai3, ...),
  #          (nilai1, nilai2, nilai3, ...),
  #          (nilai1, nilai2, nilai3, ...);
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
    self.cursor.executemany(f'INSERT INTO {table_name} VALUES ({placeholders})', data)
    return self
  
  def insert_data(self, table_name:str, data:list):
    """    
    parameter :
      table : nama tabel
      data  : (data1, data2, data3, ...)

    """
    if not self.check_conn() : return None
    # Membuat query SQL untuk memasukkan data
    query = f"INSERT INTO {table_name} VALUES ({', '.join(['?' for _ in range(len(data))])})"
    
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
  # READ (SELECT)
  #   SELECT column1, column2, ... FROM table_name WHERE condition
  #   SELECT * FROM table_name
  # ###############################################################################################

  def select_row(self,column, value, table_name=None):
    if not self.check_conn() : return None
    table_name = f'{table_name}' if table_name else f'{self.__table}'
    
    try:
      # Eksekusi perintah SQL untuk memilih satu baris dengan kondisi tertentu
      self.cursor.execute(self.q_read(
                                      table_name= table_name,
                                      condition = f'"{column}" = {value}'
                                     )
                          )      
      # Ambil satu baris yang dipilih
      row = self.cursor.fetchone()
    except sqlite3.Error as e:
      self.print_e(e)
    return row
  
  def table(self, table_name=None, columns:list=None):
    if not self.check_conn() : return None
    table_name = f'{table_name}' if table_name else f'{self.__table}'
    try:
      # Eksekusi perintah SQL untuk memilih satu baris dengan kondisi tertentu
      self.cursor.execute(self.q_read(
                                      columns=columns,
                                      table_name=table_name
                                     )
                          )
      # Ambil satu baris yang dipilih
      return self.cursor.fetchall()
    except sqlite3.Error as e:
      self.print_e(e)
    return self

  def select_top(self, n, table_name=None):
    """
    Query n first rows of the table
    parameter :
      conn: the Connection object
      table: The table to query
      n: Number of rows to query
    return :
      rows
    """
    if not self.check_conn() : return None
    table_name = f'{table_name}' if table_name else f'{self.__table}'
    self.cursor.execute(self.q_read(
                                   table_name=table_name,
                                   limit=n
                                   )
                        )
    rows = self.cursor.fetchall()
    return rows
  
  def select(self,query):
    if not self.check_conn() : return None
    self.cursor.execute(query)
    return self.cursor.fetchall()
  
  def read_sql_to_df(self, table_name=None, column:list=None):
    if not self.check_conn() : return None
    table_name = f'{table_name}' if table_name else f'{self.__table}'
    if column : 
      return pd.read_sql(self.q_read(
                                     table_name=table_name,
                                     columns=column
                                    ), self.conn)
    else :
      return pd.read_sql(f"SELECT * FROM {table_name}", self.conn)
    
  
  def read_query_to_df(self,query):
    if not self.check_conn() : return None
    return pd.read_sql_query(query, self.conn)
  
  def q_read(self,
           distinct:bool  = False,
           columns:list   = None,
           where:str      = None,
           group:str      = None,
           having:str     = None,
           order:str      = None,
           sort:str       = None,
           limit:int      = None,
           offset:int     = None,
           table_name     = None,
           ):
    """
    contoh query SELECT :
      SELECT name, age, department 
      FROM employees 
      [ WHERE department = 'Sales']
      [ GROUP BY department ]
      [ HAVING AVG(age) > 30 ]
      [ ORDER BY name ASC ]
      [ LIMIT 10 OFFSET 0;]
    parameter :
      - distinct:bool  = False,
      - columns:list   = None,
      - where:str      = None,
      - group:str      = None,
      - having:str     = None,
      - order:str      = None,
      - sort :str      = None,
      - limit:int      = None,
      - offset:int     = None,
      - table_name     = None,

    """
    tb = '\nFROM '+(f'{table_name}' if table_name else f'"{self.__table}"')
    co = ', '.join([f'"{c}"' for c in columns]) if columns else "*"
    cn = f'\nWHERE {where}' if where else ''
    di = 'DISTINCT ' if distinct else ''
    gr = f'\nGROUP BY "{group}"' if group else ''
    hv = f'\nHAVING {having}' if having else ''
    od = f'\nORDER BY "{order}"' if order else ''
    ac = f' {sort}' if sort else ''
    li = f'\nLIMIT {limit}' if limit else ''
    of = f' OFFSET {offset}' if offset else ''
    
    q = f"SELECT {di}{co}{tb}{cn}{gr}{hv}{od}{ac}{li}{of};"
    return q

  # ###############################################################################################
  # UPDATE (UPDATE)
  #   UPDATE table_name SET column1 = value1, column2 = value2, ... WHERE condition
  # ###############################################################################################

  def update_data(self, table_name:str, set_values:dict, condition:str):
    """
      Metode untuk melakukan operasi update pada data dalam tabel.

      Parameters:
      - table: Nama tabel yang akan diupdate.
      - set_values: Dictionary yang berisi pasangan kolom dan nilai yang akan diubah.
      - condition: Kondisi untuk mengidentifikasi baris yang akan diupdate.

      Contoh penggunaan:
      db.update_data('users', {'name': 'John Doe', 'age': 35}, 'id = 1')
    """
    if not self.check_conn() : return None
    set_clause = ", ".join([f"{column} = ?" for column in set_values.keys()])
    query = f"UPDATE {table_name} SET {set_clause} WHERE {condition};"
    try:
      self.cursor.execute(query, list(set_values.values()))
      self.conn.commit()
      print(f"Data {set_clause} updated successfully!")
    except sqlite3.Error as e:
      self.print_e(e)
      self.conn.rollback()
      print("data is returned to original state")
    return self

  # ###############################################################################################
  # DELETE (DELETE)
  #   DELETE FROM table_name WHERE condition
  # ###############################################################################################
  
  def delete_row(self,table_name:str, condition=None):
    '''
    sebelum delete pastikan cek data lewat metode
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
  # 
  # Membuat Table :
  #   ```
  #   CREATE TABLE IF NOT EXISTS users (
  #     id INTEGER PRIMARY KEY,
  #     name TEXT NOT NULL,
  #     age INTEGER
  #   );
  #   ```
  #
  # Menambahkan data ke tabel :
  #   ```
  #   q = "INSERT INTO users (name, age) VALUES (?, ?);""
  #   execute_query(q, ('alice', 30))
  #   ```
  #
  # Melakukan query untuk mendapatkan data :
  #   ```
  #   q = SELECT * FROM users;
  #   result = db.execute_query(q)
  #   ```
  # ###############################################################################################

  def execute_query(self, query, parameters=None):
    if not self.check_conn() : return None
    try :
      if parameters :
        self.cursor.execute(query, parameters)
      else :
        self.cursor.execute(query)
      self.conn.commit()
      return self.cursor.fetchall()
    except sqlite3.Error as e:
      self.print_e(e)
      self.conn.rollback()
      print("data is returned to original state")
    return self
  
  # ###############################################################################################
  # GENERAL
  # ###############################################################################################

  def set_table(self, table_name):
    self.__table = table_name
    return self

  def print_e(self,e):
     print("Error:", e)
     return self