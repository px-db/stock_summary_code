import os
raw_pxdb   = 'https://raw.githubusercontent.com/px-db/'
full_col   = '../full_col/'
short_col  = '../short_col/'
repo_smi   = '../stock_summary_idx/'

def islist(test):
  if not isinstance(test, (list)) :
    print('attribut bukan list')
    return False
  return True

def is_idx_calendar(test):
  if not isinstance(test, (list)) :
    print('attribut bukan IDX_Calendar')
    return False
  return True

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

def in_list(test, list_to_test):
  if test in list_to_test :
    return True
  return False

def key_in_dict(key_to_check, dict_to_test:dict):
  if key_to_check in dict_to_test.keys() :
    return True
  return False

def create_dir_if_not_exist(dir_name:str,
                            path_dir:str = 'stock_summary_idx'):
  if not os.path.exists(os.path.join,(path_dir,dir_name)):
    os.makedirs(os.path.join(path_dir,dir_name))