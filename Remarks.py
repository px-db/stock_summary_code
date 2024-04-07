import pandas as pd

class Remarks_idx :
  """
  digit 1 sampai 5 :
    - Informasi Perusahaan
  digit 6 smapai 14 :
    - Informasi Keanggotaan indeks
  digit 15 sampai 18 :
    - Informasi Sektor
  digit 19 sampai 30 :
    - Informasi Notasi Khusus
    
  Referensi :
  - https://www.idx.co.id/Media/2sdcgif5/se_00015_bei_2023_tampilan_informasi_perusahaan_tercatat_pada_kolom_remarks_dalam_jats-pdf.pdf
  - https://www.idx.co.id/id/perusahaan-tercatat/notasi-khusus
  """
  def __init__(self, remarks=None) -> None:
    self.__result = None
    self.marks = {}
    pass
    
  def get_info(self, remarks:str):
    if len(remarks) != 30 : return False
    info = {
      1 : '1.'+self.mid(remarks,1,2),
      3 : '3.'+self.mid(remarks,3,1),
      4 : '4.'+self.mid(remarks,4,1),
      5 : '5.'+self.mid(remarks,5,1),
    }
    self.marks.update(info)
    self.__result = self.marks
    return info
    
  def get_indeks(self, remarks:str):
    if len(remarks) != 30 : return False
    indeks ={
      6  : '6.' + self.mid(remarks,6,1) ,
      7  : '7.' + self.mid(remarks,7,1) ,
      8  : '8.' + self.mid(remarks,8,1) ,
      9  : '9.' + self.mid(remarks,9,1) ,
      10 : '10.'+ self.mid(remarks,10,1),
      11 : '11.'+ self.mid(remarks,11,1),
      12 : '12.'+ self.mid(remarks,12,1),
      13 : '13.'+ self.mid(remarks,13,1),
      14 : '14.'+ self.mid(remarks,14,1),
    }
    self.marks.update(indeks)
    self.__result = self.marks
    return indeks

  def get_sector(self, remarks:str):
    if len(remarks) != 30 : return False
    indeks = {15:'15.'+ self.mid(remarks,15,1)}
    self.marks.update(indeks)
    self.__result = self.marks
    return indeks

  def get_notasi(self, remarks:str):
    if len(remarks) != 30 : return False
    notasi = {
    19 : '19.' + self.mid(remarks,19,1), 
    20 : '20.' + self.mid(remarks,20,1), 
    21 : '21.' + self.mid(remarks,21,1), 
    22 : '22.' + self.mid(remarks,22,1), 
    23 : '23.' + self.mid(remarks,23,1), 
    24 : '24.' + self.mid(remarks,24,1), 
    25 : '25.' + self.mid(remarks,25,1), 
    26 : '26.' + self.mid(remarks,26,1), 
    27 : '27.' + self.mid(remarks,27,1), 
    28 : '28.' + self.mid(remarks,28,1), 
    29 : '29.' + self.mid(remarks,29,1), 
    30 : '30.' + self.mid(remarks,30,1),
    }
    self.marks.update(notasi)
    self.__result = self.marks
    return notasi

  def mid(self,text:str, position:int, length:int):
    return text[position-1:position+length-1]
