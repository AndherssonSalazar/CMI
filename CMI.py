import traceback
import pandas as pd
from modules.inputs import Inputs
from modules.report import Report
from modules.process import Process

if __name__ =='__main__':
    try:
        print('=======================================================================')
        print('[INFO] Lectura de datos')
        inputs = Inputs()
        print('=======================================================================')
        print('[INFO] Procesando Información')
        process=Process(inputs)
        print('=======================================================================')
        print('[INFO] Generando reporte')
        report = Report(process)
        report.save_report()
        print('=======================================================================')
        input('[SUCESS] Presionar enter para cerrar...')
    except Exception as e:
        traceback.print_exc()
        print(e)
        input('[ERROR] Presione enter para cerrar la consola error en el proceso...')
# pyinstaller --hidden-import=pkg_resources.py2_warn --onefile --clean 