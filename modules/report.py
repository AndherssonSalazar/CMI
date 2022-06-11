import openpyxl
import datetime
import pandas as pd
from openpyxl.styles import PatternFill
from modules.params import (
    NAME_REPORT_EXPORT,
    NAME_REPORT_EXPORT_BRANCH,
    NAME_REPORT_BRANCHS,
    NAME_REPORT_TRUCKS_CONSOLIDATION,
    NAME_REPORT_ERROR
)
class Report:
    def __init__(self, cant_gate) -> None:
        self.cant_gate = cant_gate
    def _change_value(self, value):
        return setting_value.get(value,value)
    def _header_style(self, df):
        return df.style.set_table_styles([{
       'selector': 'th',
       'props': [('background-color', '#add8e6')]
       }])
    def save_report(self):
        self.cant_gate.get_data().rename (columns = {'AjustePallet':'Compra Final', 'VolumenFinalTotal':'Volumen Final', 'NCajasPicking':'Picking'}).to_excel(NAME_REPORT_EXPORT)
        self.cant_gate.get_branchs_data().rename (columns = {'Volumen':'Volumen Inicial', 'VolumenAumentado':'Ajuste Volumen', 'Camion':'Camión', 'DOHInicial':'DOH Inicial', 'DOHFinal':'DOH Final', 'AjustePallet':'Compra Final S/.', 'VolumenFinal':'Volumen Final', 'NCajasPicking':'Cajas Picking'}).to_excel(NAME_REPORT_BRANCHS)
        #self.cant_gate.get_trucks_consolidate().to_excel(NAME_REPORT_TRUCKS_CONSOLIDATION)
    def save_error(self, errors):
        errors.to_excel(NAME_REPORT_ERROR)