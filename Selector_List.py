from Common import islist, convert_hexa_to_bitlist

class Selector_List :
  '''
  Class ini berfungsi untuk memilih parameter dalam list hanya dengan
  memasukan parameter hexa(string). Pertama kita harus mendefinisikan
  list.
  
  Contoh :
  List pertama sebagai LSB

  list = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J']

  hexa = '1'   ;  result = ['A']
  hexa = 'A'   ;  result = ['B','D']
  hexa = 'F'   ;  result = ['A', 'B', 'C', 'D']
  hexa = '11'  ;  result = ['A','E']
  hexa = '288' ;  result = ['D','H','J']

  '''
  usecols = [
      # xxxx.xxxN
      "No", # 1
      "Stock Code", # 2 default
      "Company Name", # 4
      "Remarks", # 8 default
      # xxxx.xxNx
      "Previous", # 1 default
      "Open Price", # 2 default
      "Last Trading Date", # 4 default
      "First Trade", # 8
      # xxxx.xNxx
      "High", # 1 default
      "Low", # 2 default
      "Close", # 4 default
      "Change", # 8 default
      # xxxx.Nxxx
      "Volume", # 1 default
      "Value", # 2 default
      "Frequency", # 4 default
      "Index Individual", # 8
      # xxxN.xxxx
      "Offer", # 1
      "Offer Volume", # 2
      "Bid", # 4
      "Bid Volume", # 8
      # xxNx.xxxx
      "Listed Shares", # 1
      "Tradeble Shares", # 2
      "Weight For Index", # 4
      "Foreign Sell", # 8
      # xNxx.xxxx
      "Foreign Buy", # 1
      "Non Regular Volume", # 2
      "Non Regular Value", # 4
      "Non Regular Frequency", # 8
    ]
  def __init__(self, cols:list=None, mask:str='7F7A'):
    if cols != None and islist(cols):
      self.usecols = cols
    self.select_cols(mask)  

  def set_cols (self, cols:list):
    if islist(cols) :
      return self
    self.usecols = cols
    return self

  def select_col(self,mask:str='7F7A') :
    if len(self.usecols) < len(mask)*16 :
      print(f'mask maksimal {len(self.usecols)}')
    self.result = []
    count = 0
    for bit in convert_hexa_to_bitlist(mask) :
      if bit == '1' :
        self.result.append(self.usecols[count])
      count += 1
      if count == 28 :
        break
    return self