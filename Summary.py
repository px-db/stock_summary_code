import pandas as pd
from IDX_Calendar import IDX_Calendar
from Common import raw_pxdb, key_in_dict, repo_smi
#from Selector_List import Selector_List as sl

class Summary :
  '''
  Class Summary adalah kumpulan dataframe yang sudah di kelompokan berdasarkan
  harian, bulanan dan tahunan.
  '''
  def __init__(self,calendar:IDX_Calendar):
    #self.cols = sl().result
    self.idxcal = calendar
    self.dfs_days = self.list_dfs()
    self.monthly = {}
    self.annually = {}
    self.set_periode_df()
  
  def list_dfs(self,cal:str='filter')->dict[str, pd.DataFrame]:
    dict_df = {}
    if cal == 'filter' :
      dates = self.idxcal.filter
    if cal == 'full' :
      dates = self.idxcal.full
    for d in dates :
      dict_df[d] = pd.read_csv(f'{repo_smi}short_col/{d[:4]}/stock_summary_{d}.csv',
                                     index_col = 'Stock Code'
                                     )
      dict_df[d].loc[:,('No')] = self.idxcal.count_no[d]
    return dict_df

  def set_periode_df (self):
    periode = [self.idxcal.monthly, self.idxcal.annually]
    data = [] # {'yyyymm':df}
    for p in periode :
      for k, dates in p.items():
        data=[]
        for d in dates :
          data.append(self.dfs_days[d])
        if len(k) == 6 :
          self.monthly[k] = self.concat_df(data)
        if len(k) == 4 :
          self.annually[k] = self.concat_df(data)
    return self

  def concat_df(self,
                list_dfs:list[pd.DataFrame],
                ) -> pd.DataFrame:
    return pd.concat(list_dfs,
                     ignore_index=False
                     )

  def set_df_summary_dict(self,dfpy:dict): # dfpy hasil concat_df()
    '''
    parameter :
  
    return :
      dict
    '''
    summary = {}
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
    # Key bisa tahun atau bulan
    for key, df_py in dfpy.items():
      count_code = pd.Series(df_py.index.tolist()).value_counts()
      for code in df_py.index.unique().tolist() :
        if not (code in df_py.index.tolist()) :
          continue
        if count_code[code] == 1:
          continue
        if not key_in_dict(code,first_prev) :
          first_prev[code] = int(df_py.loc[code].iloc[0]['Prev'])

        chg = round(((df_py.loc[code].iloc[-1]['Close']/df_py.loc[code].iloc[0]['Prev'])-1.0)*100,2)
        chg_ytm = round(((df_py.loc[code].iloc[-1]['Close']/first_prev[code])-1.0)*100,2)
        start_trading = df_py.loc[code].iloc[0]['Date']
        end_trading = df_py.loc[code].iloc[-1]['Date']
        tot_value = df_py.loc[code]['Value'].sum()
        tot_vol = df_py.loc[code]['Volume'].sum()
        tot_freq = df_py.loc[code]['Freq'].sum()
        sector = str(df_py.loc[code].iloc[-1]['Remarks'])[14:18]

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
             'Month'           : str(key[4:6])
             }
        )
      summary[key] = list_summary
      list_summary = []
    return summary