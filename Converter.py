import pandas as pd
import os

class Converter_SS :
  '''
  Converter Stock Summary
  '''
  local_dir = 'download/'
  file_dl = []
  save_to = '../stock_summary_idx/full_col/'
  def __init__(self,dir:str=local_dir):
    for f in os.listdir(os.path.join(dir)):
      self.file_dl.append(f)
  def convert_xslx_to_csv(self):
    for file in self.file_dl :
      pd.read_excel(
        os.path.join(self.local_dir,file)).to_csv(
         os.path.join(self.save_to,
                      f'{file[14:18]}',
                      f'stock_summary_{file[14:22]}.csv'),
                     index=False)
      print(os.path.join(self.save_to,
                         f'{file[14:18]}',
                         f'stock_summary_{file[14:22]}.csv sudah disimpan'))
    return self