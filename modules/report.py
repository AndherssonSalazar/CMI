import openpyxl
import datetime
import pandas as pd
from openpyxl.styles import PatternFill
from modules.params import (
    NAME_REPORT_EXPORT,
    NAME_REPORT_EXPORT_BRANCH,
    NAME_REPORT_BRANCHS,
    NAME_REPORT_TRUCKS_CONSOLIDATION
)
class Report:
    def __init__(self, cant_gate) -> None:
        self.cant_gate = cant_gate
    def _change_value(self, value):
        return setting_value.get(value,value)
    def save_report(self):
        self.cant_gate.get_data().to_excel(NAME_REPORT_EXPORT)
        self.cant_gate.get_branchs_data().to_excel(NAME_REPORT_BRANCHS)
        self.cant_gate.get_trucks_consolidate().to_excel(NAME_REPORT_TRUCKS_CONSOLIDATION)