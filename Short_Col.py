import pandas as pd # v 1.1.5
from IDX_Calendar import IDX_Calendar
from Selector_List import Selector_List as sl
from Common import full_col, short_col, repo_smi
# Belum d test
class Short_Col :
  '''
  Class ini untuk mengubah dan meringkas kolom dari summary yang ada di full kolom.
  
  '''
  def __init__(self,start_date='2023', end_date='2023') :
    cols = sl().result
    self.df = {}
    for d in IDX_Calendar(start_date = start_date,
                          end_date   = end_date).filter :
      self.df[d]=pd.read_csv(f'{full_col}{d[:4]}/stock_summary_{d}.csv',
                                      usecols = cols,
                                      index_col = 'Stock Code'
                                     ).rename(columns={
                                                       'Previous':'Prev',
                                                       'Open Price':'Open',
                                                       'Last Trading Date':'Date',
                                                       'Frequency':'Freq'
                                                      }
                                              )
      self.df[d].loc[:,('year')] = d[:4]
      self.df[d].loc[:,('month')] = d[4:6]

  def save_csv(self,
               repo:str    = repo_smi,
               dir_col:str = short_col,
              ) :
    save_to = f'{repo}{dir_col}'
    count = 0
    for d, df in self.df.items():                                              
      df.to_csv(f'{save_to}/{d[:4]}/stock_summary_{d}.csv')      
      print(f'{count}',end='')      
      if count==9 :
        count = 0
        print('/')
        continue
      count += 1
    return self