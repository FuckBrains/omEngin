import xlrd
import os
pt = os.getcwd()
excelpath = pt + '\\xlsF\\A_SEMRW.xlsm'
filepath= pt + '\\download\\0730200157.csv'
excel_app = xlwings.App(visible=False)
excel_book = excel_app.books.open(excelpath)
# into brackets, the path of the macro
x = excel_book.macro('init')
x(filepath)