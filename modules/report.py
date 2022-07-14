import openpyxl
import datetime
import pandas as pd
import os
from openpyxl.styles import PatternFill
from modules.params import (
    NAME_DF_AGA,
    NAME_DF_AB,
    NAME_DF_DIJISA,
    NAME_DF_VEGA,
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
    def _renameColumnsData(self):
        return self.process.get_data().rename(columns = {'AjustePallet':'Compra Final', 'VolumenFinalTotal':'Volumen Final', 'NCajasPicking':'Picking'})
    def _renameColumnsBranch(self):
        return self.process.get_branchs_data().rename (columns = {'Volumen':'Volumen Inicial', 'VolumenAumentado':'Ajuste Volumen', 'Camion':'Camión', 'DOHInicial':'DOH Inicial', 'DOHFinal':'DOH Final', 'CompraFinal':'Compra Final S/.', 'VolumenFinal':'Volumen Final', 'NCajasPicking':'Cajas Picking'})
    def save_report(self):
        if self.process.is_automatic():
            self._renameColumnsData().to_excel(os.path.join(NAME_FOLDER_REPORT, self.process.getInputs().getDirectories()[self.process.getInputs().getNumberOption()-1],'Formato_2.11_'+self.process.getInputs().getDirectories()[self.process.getInputs().getNumberOption()-1]+'_export.xlsx'))
            self._renameColumnsBranch().to_excel(os.path.join(NAME_FOLDER_REPORT, self.process.getInputs().getDirectories()[self.process.getInputs().getNumberOption()-1], self.process.getInputs().getDirectories()[self.process.getInputs().getNumberOption()-1]+NAME_REPORT_BRANCHS))
        else:
            if self.process.getInputs().getNumberOption()==1:
                self._renameColumnsData().to_excel(os.path.join(NAME_FOLDER_REPORT, NAME_DF_AGA))
                self._renameColumnsBranch().to_excel(os.path.join(NAME_FOLDER_REPORT, 'AGA', NAME_REPORT_BRANCHS))
            elif self.process.getInputs().getNumberOption()==2:
                self._renameColumnsData().to_excel(os.path.join(NAME_FOLDER_REPORT, NAME_DF_AB))
                self._renameColumnsBranch().to_excel(os.path.join(NAME_FOLDER_REPORT, 'Alvarez Bohl', NAME_REPORT_BRANCHS))
            elif self.process.getInputs().getNumberOption()==3:
                self._renameColumnsData().to_excel(os.path.join(NAME_FOLDER_REPORT, NAME_DF_DIJISA))
                self._renameColumnsBranch().to_excel(os.path.join(NAME_FOLDER_REPORT, 'Dijisa', NAME_REPORT_BRANCHS))
            elif self.process.getInputs().getNumberOption()==4:
                self._renameColumnsData().to_excel(os.path.join(NAME_FOLDER_REPORT, NAME_DF_VEGA))
                self._renameColumnsBranch().to_excel(os.path.join(NAME_FOLDER_REPORT, 'Grupo Vega', NAME_REPORT_BRANCHS))
    def save_error(self, errors):
        errors.to_excel(NAME_REPORT_ERROR)