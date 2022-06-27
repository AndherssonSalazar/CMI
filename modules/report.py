import openpyxl
import datetime
import pandas as pd
import os
from openpyxl.styles import PatternFill
from modules.params import (
    NAME_FOLDER_REPORT,
    NAME_REPORT_BRANCHS,
    NAME_REPORT_ERROR
)
class Report:
    def __init__(self, process) -> None:
        self.process = process
    def _header_style(self, df):
        return df.style.set_table_styles([{
       'selector': 'th',
       'props': [('background-color', '#add8e6')]
       }])
    def save_report(self):
        self.process.get_data().rename (columns = {'AjustePallet':'Compra Final', 'VolumenFinalTotal':'Volumen Final', 'NCajasPicking':'Picking'}).to_excel(os.path.join(NAME_FOLDER_REPORT, self.process.getInputs().getDirectories()[self.process.getInputs().getNumberOption()-1],'Formato_2.11_'+self.process.getInputs().getDirectories()[self.process.getInputs().getNumberOption()-1]+'_export.xlsx'))
        self.process.get_branchs_data().rename (columns = {'Volumen':'Volumen Inicial', 'VolumenAumentado':'Ajuste Volumen', 'Camion':'Camión', 'DOHInicial':'DOH Inicial', 'DOHFinal':'DOH Final', 'AjustePallet':'Compra Final S/.', 'VolumenFinal':'Volumen Final', 'NCajasPicking':'Cajas Picking'}).to_excel(os.path.join(NAME_FOLDER_REPORT, self.process.getInputs().getDirectories()[self.process.getInputs().getNumberOption()-1], self.process.getInputs().getDirectories()[self.process.getInputs().getNumberOption()-1]+NAME_REPORT_BRANCHS))
    def save_error(self, errors):
        errors.to_excel(NAME_REPORT_ERROR)