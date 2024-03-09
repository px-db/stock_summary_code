import pandas as pd
import os
#from IDX_Calendar import IDX_Calendar
#from Common import raw_pxdb, key_in_dict, repo_smi
#from Selector_List import Selector_List as sl

path_full_col = '../stock_summary_idx/full_col'
path_short_col = '../stock_summary_idx/short_col'
root_ssi      = '../stock_summary_idx'
path_summary = '../stock_summary_idx//summary'
raw_pxdb = 'https://raw.githubusercontent.com/px-db/stock_summary_idx/main'

def in_list(test, list_to_test):
  '''
  Cek apakah value 'test' ada di dalam list 'list_to_test'
  '''
  if test in list_to_test :
    return True
  return False

def key_in_dict(key_to_check, dict_to_test:dict):
  '''
  Cek apakah value 'key_to_check' ada di dalam key dictionary 'dict_to_test'
  '''
  if key_to_check in dict_to_test.keys() :
    return True
  return False

def convert_hexa_to_bitlist(hexa):
  '''
  Fungsi ini untuk konversi hexa dalam string menjadi bit dalam list
  dengan urutan Low Significant Bit (LSB) terlebih dahulu.

  Contoh :
  'A' = 1010 ; return [0,1,0,1]

  Parameter :
    hexa:str
  
  return : list
  '''
  # convert to bit
  bit = bin(int(hexa,16))
  len_x = len(str(bit)[2:])

  # add padding in front
  pad = '0'*(32-len_x)+str(bit)[2:]

  # list in reverse list
  return list(pad)[::-1]

class Summary :
  '''
  Class Summary adalah kumpulan dataframe yang sudah di kelompokan berdasarkan
  harian, bulanan dan tahunan.
  Property :
    date     : IDX_Calendar
    dfs_days : {'yyyymmdd':df}
    monthly  : {'yyyymm':df,...}
    annually : {'yyyy':df,...}
    monthly_summary  : {'yyyymm': df,...}
    annually_summary : {'yyyy': df,...}
    year_to_month : {'yyyymm':df}
    summary : {'yyyymm':df...,'yyyy':df}
  '''
  def __init__(self,
               #calendar:IDX_Calendar,
               mode = 'local',
               start_date='2019', 
               end_date = '9999'):
    #self.cols = sl().result
    #self.idxcal = calendar
    self.mode = mode
    self.dfs_days = self.list_dfs()
    self.monthly_df = {}
    self.annually_df = {}
    self.set_periodic_df()
    self.summary = {}

    #========================================
    self.mode = mode
    self.set_periodic_cal(start_date=start_date, end_date=end_date)
  
  def list_dfs(self,cal:str='filter')->dict[str, pd.DataFrame]:
    if self.mode == 'local'  : root = root_ssi
    if self.mode == 'remote' : root = raw_pxdb
    if cal == 'filter'       : dates = self.filter_cals
    if cal == 'full'         : dates = self.full_cals
    dict_df = {}
    for d in dates :
      dict_df[d] = pd.read_csv(f'{root}/short_col/{d[:4]}/stock_summary_{d}.csv',
                                     index_col = 'Stock Code'
                                     )
      dict_df[d].loc[:,('No')] = self.count_no_cals[d]
    return dict_df

  def set_periodic_df (self):
    '''
    
    '''
    periode = [self.monthly_cals, self.annually_cals]
    data = []
    for p in periode :
      for k, dates in p.items():
        data=[]
        for d in dates :
          data.append(self.dfs_days[d])
        if len(k) == 6 :
          self.monthly_df[k] = self.concat_df(data)
        if len(k) == 4 :
          self.annually_df[k] = self.concat_df(data)
    return self

  def concat_df(self,
                list_dfs:list[pd.DataFrame],
                ) -> pd.DataFrame:
    return pd.concat(list_dfs,
                     ignore_index=False
                     )
  
  def save_summary_tocsv(self):
    for k, v in self.summary.items():
      pd.DataFrame(v).to_csv(f'{path_summary}/summary_{k}.csv')
      print(f'{path_summary}/summary_{k}.csv sudah disimpan')
    return self

  def create_summary(self):
    '''
     
    return :
      dict
    '''
    self.summary = {}
    list_summary = []
    chg = 0.0
    chg_ytm = 0.0
    start_trading = ''
    end_trading = ''
    tot_value = 0
    tot_vol = 0
    tot_freq = 0
    sector = ''
    count_code = {}
    first_prev = {}
    month = ''
    # Key bisa tahun atau bulan
    for dfpy in [self.monthly_df, self. annually_df]:
      for key, df_py in dfpy.items():
        count_code = pd.Series(df_py.index.tolist()).value_counts()
        for code in df_py.index.unique().tolist() :
          if not in_list(code, df_py.index.tolist()) : continue
          if count_code[code] == 1 : continue
          if not key_in_dict(code,first_prev) :
            if 'Prev' in df_py.columns.tolist() :
              first_prev[code] = int(df_py.loc[code].iloc[0]['Prev'])
            else :
              first_prev[code] = int(df_py.loc[code].iloc[0]['Previous'])
          if 'Prev' in df_py.columns.tolist() :
            chg = round(((df_py.loc[code].iloc[-1]['Close']/df_py.loc[code].iloc[0]['Prev'])-1.0)*100,2)
            start_trading = df_py.loc[code].iloc[0]['Date']
            end_trading = df_py.loc[code].iloc[-1]['Date']
            tot_freq = df_py.loc[code]['Freq'].sum()
          else :
            chg = round(((df_py.loc[code].iloc[-1]['Close']/df_py.loc[code].iloc[0]['Previous'])-1.0)*100,2)
            start_trading = df_py.loc[code].iloc[0]['Last Trading Date']
            end_trading = df_py.loc[code].iloc[-1]['Last Trading Date']
            tot_freq = df_py.loc[code]['Frequency'].sum()
          chg_ytm = round(((df_py.loc[code].iloc[-1]['Close']/first_prev[code])-1.0)*100,2)
          tot_value = df_py.loc[code]['Value'].sum()
          tot_vol = df_py.loc[code]['Volume'].sum()
          sector = str(df_py.loc[code].iloc[-1]['Remarks'])[14:18]
          if len(key) == 4 :
            month = 0
          if len(key) == 6 :
            month = str(key[4:6])
          list_summary.append(
              {'Stock Code'      : code,
               'change %'        : chg,
               'change YtM %'    : chg_ytm,
               'Total Trading'   : count_code[code],
               'Start Trading'   : str(start_trading),
               'End Trading'     : str(end_trading),
               'Total Value'     : tot_value,
               'Total Volume'    : tot_vol,
               'Total Frequency' : tot_freq,
               'Sektor ID'       : sector,
               'Year'            : str(key[:4]),
               'Month'           : month
               }
          )
        self.summary[key] = list_summary
        list_summary = []
    return self
  
  # ==============================================================

  def scan_dir_download(self):
    '''
    # 01
    Download terlebih dahulu stock summary di :
      https://www.idx.co.id/en/market-data/trading-summary/stock-summary
    dan simpan di direktori 'download'.    
    return :
      dict{
        'Stock Summary-20240206.xlsx':'../stock_summary_idx/full_col/2024/stock_summary_20240206.csv',
        'Stock Summary-20240207.xlsx':'../stock_summary_idx/full_col/2024/stock_summary_20240207.csv',
        ...      
      }
    '''
    self.dict_download = {}
    for file in os.listdir('download'):
      self.dict_download[file] = f'{path_full_col}/{file[14:18]}/stock_summary_{file[14:22]}.csv'
    return self

  def convert_xlsx_to_csv(self,
                          file_xlsx=None
                          ):
    '''
    # 02
    Proses konversi xlsx ke csv.

    Converter Stock Summary
    'Stock Summary-20240206.xlsx' -> stock_summary_20240206.csv
    '''
    self.scan_dir_download()
    if len(self.dict_download) == 0 :
      print('tidak ada file di direktori download')
      return None
    # Read xlsx and convert csv
    for file_xlsx, file_csv in self.dict_download.items():
      pd.read_excel(file_xlsx).to_csv(file_csv, index=False)
      print(file_csv)
    return self
  
  def update_file_calendar(self):
    '''
    # 03
    update file kalender_market_idx.csv
    '''
    # Cek existing calender
    scan_cal = {
                'kalender':[],
                'jumlah emiten':[],
               }

    for y in os.listdir(path_full_col)[::-1]:
      for f in os.listdir(f'{path_full_col}/{y}')[::-1]:
        if not in_list(f[14:22],self.full_cals) :
          scan_cal['kalender'].append(int(f[14:22]))
          scan_cal['jumlah emiten'].append(len(f'{path_full_col}/{y}/{f}'))
          continue
        break

    if scan_cal == 0 :
      print('file kalender_market_idx.csv sudah update')
      return self
    
    df = pd.DataFrame(scan_cal).sort_values('kalender')

    # update file kalender_market_idx.csv
    df.to_csv(f'{root_ssi}/kalender_market_idx.csv', mode='a', index=False, header=False)
    return self

  def update_full_cal(self):
    '''
    # 04
    Jika sudah melakukan update kalendar update full_cals, dengan memanggil fungsi ini.
    '''
    # update kalender
    self.full_cals = [str(d) for d in pd.read_csv(f'{root_ssi}/kalender_market_idx.csv')['kalender'].tolist()]
    return self
  
  def set_periodic_cal(self,
                       start_date= '2019',
                       end_date = '9999'
                       ):
    '''
    # 05
    full : ['yyyymmdd', ...]
    months : ['yyyymm', ...]
    years : ['yyyy', ...]
    annually : {'yyyy':'yyyymmdd', ...}
    monthly : {'yyyymm':'yyyymmdd', ...}
    filter : ['yyyymmdd', ...]
    count_no = {'yyyymmdd':1, 'yyyymmdd':2, ...}
    '''
    self.update_full_cal()
    self.months_cals = []
    self.years_cals = []
    self.annually_cals = {}
    self.monthly_cals ={}
    self.count_no_cals = {}
    self.filter_cals = []

    for i in self.full_cals :
      if i[:4] not in self.years :
        self.years.append(i[:4])
      if i[:6] not in self.years :
        self.months.append(i[:6])
    self.months = list(set(self.months))
    self.years = list(set(self.years))

    # Filtering
    x = len(start_date)
    if x != len(end_date) and end_date != "9999":
      end_date += "12"
    self.filter_cals = list((d for d in self.full_cals if start_date <= d[:x] <= end_date))

    for i in self.filter :
      if i[:4] in self.annually :
        self.annually[i[:4]].append(i)
      else :
        self.annually[i[:4]] = [i]
      if i[:6] in self.monthly:
        self.monthly[i[:6]].append(i)
      else :
        self.monthly[i[:6]] = [i]

    # Count Number
    count = 1
    prev_month = '01'
    for i in self.filter_cals :
      if i[4:6] != prev_month :
        count = 1
      self.count_no[i] = count
      count += 1
      prev_month = i[4:6]
  
  def set_list_col(self, path_file = path_full_col):
    '''
    List kolom default ambil dari full_col
    '''

    last_year  = os.listdir(f'{path_file}')[-1]
    last_file= os.listdir(f'{path_file}/{last_year}/')[-1]
    self.list_cols = pd.read_csv(f'{path_file}/{last_year}/{last_file}').columns.tolist()
    return self
  
  def select_col(self,mask:str='7F7A') :
    '''
    panggil metode self.set_list_full_col() sebelum eksekusi ini.
    dan pastikan self.list_cols sudah update.
    
    '''
    if len(self.list_cols) < len(mask)*16 :
      print(f'mask maksimal {len(self.list_cols)}')
    result = []
    count = 0
    for bit in convert_hexa_to_bitlist(mask) :
      if bit == '1' :
        result.append(self.list_cols[count])
      count += 1
      if count == 28 :
        break
    return result

    
  def full_to_short_col (self):
    '''
    # 06
    Meringkas kolom file csv di direktori full_col ke direktori short_col
    
    '''
    # set col
    self.set_list_col()
    cols = self.select_col()

    # Cek file existing
    existing_short_col = []

    for y in os.listdir(path_full_col)[::-1]: # list year
      existing_short_col = os.listdir(f'{path_short_col}/{y}/')
      for file in os.listdir(f'{path_full_col}/{y}')[::-1]:
        if not in_list(file,existing_short_col) :
          df=pd.read_csv(f'{path_full_col}/{y}/{file}',
                                      usecols = cols,
                                      index_col = 'Stock Code'
                                     ).rename(columns={
                                                       'Previous':'Prev',
                                                       'Open Price':'Open',
                                                       'Last Trading Date':'Date',
                                                       'Frequency':'Freq'
                                                      }
                                              )
          df.loc[:,('year')] = y
          df.loc[:,('month')] = file[18:20]
          df.to_csv(f'{path_short_col}/{y}/{file}')
          print(f'{file} sudah di simpan')
          del df
          continue
        break
