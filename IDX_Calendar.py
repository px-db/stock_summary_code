import pandas as pd
import os

class IDX_Calendar :
  raw_pxdb     = 'https://raw.githubusercontent.com/px-db/'
  repo         = 'stock_summary_idx/main/'
  local_dir    = '../stock_summary_idx/'
  file_cal_csv = 'kalender_market_idx.csv'
  def __init__( self,
                start_date:str = "2019",
                end_date:str   = "9999",
                file_csv:str   = raw_pxdb+repo+file_cal_csv
               ):
    '''
    Property :
      full : ['yyyymmdd', ...]
      months : ['yyyymm', ...]
      years : ['yyyy', ...]
      annually : {'yyyy':'yyyymmdd', ...}
      monthly : {'yyyymm':'yyyymmdd', ...}
      filter : ['yyyymmdd', ...]
      count_no = {'yyyymmdd':1, 'yyyymmdd':2, ...}
    '''

    self.full = [str(d) for d in pd.read_csv(file_csv)['kalender'].tolist()]
    self.months = []
    self.years = []
    self.annually = {}
    self.monthly ={}
    self.count_no = {}

    for i in self.full :
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
    self.filter = list((d for d in self.full if start_date <= d[:x] <= end_date))

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
    for i in self.filter :
      if i[4:6] != prev_month :
        count = 1
      self.count_no[i] = count
      count += 1
      prev_month = i[4:6]
  
  def scan_calendar(self, dir:str=local_dir):
    self.scanned_file = {
                'kalender':[],
                'jumlah emiten':[], 
               }
    for y in os.listdir(os.path.join(dir,'full_col')):
      for f in os.listdir(os.path.join(dir,'full_col',y)):
        self.scanned_file['kalender'].append(int(f[14:22]))
        self.scanned_file['jumlah emiten'].append(len(pd.read_csv(os.path.join(dir,'full_col',y,f))))
    self.df_calendar = pd.DataFrame(self.scanned_file).sort_values('kalender')
    return self
    
  def save_scan_calendar(self,
                         save_to:str=local_dir,
                         ):
    self.df_calendar.to_csv(os.path.join(save_to,self.file_cal_csv), index=False)
    return self