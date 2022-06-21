from math import prod
import os
import pandas as pd
import pathlib
from modules.params import(
    NAME_FOLDER_OUTPUTS,
    NAME_DF_DATA_TRUCK,
    NAME_DF_DATA_CLIENT,
    NAME_DF_SKU_DATA,
    NAME_DF_DATA_WEIGHT_VOLUME,
    NAME_DF_DATA_CMI
)
class Inputs:
    def __init__(self) -> None:
        print('==>[INFO] ELija una opcion')
        directorio = pathlib.Path('Outputs')
        self._ficheros = [fichero.name for fichero in directorio.iterdir() if not fichero.is_file()]
        if len(self._ficheros)<=0:
            raise ValueError("[Error]======No existen carpetas dentro de Outputs ======[Error]")
        for i in range(len(self._ficheros)):
            print("==>==> ["+str(i+1)+"]. "+self._ficheros[i])
        self._numberOption=1
        incorrect=True
        while incorrect:
            try:
                self._numberOption=int(input('==>==>[INPUT] Escribe el numero de la sucursal por favor: '))
                if self._numberOption>0 and self._numberOption<=len(self._ficheros):
                    incorrect=False
                else:
                    print('==>==>[INFO] El numero de opcion que escribiste no es correcto')
            except Exception as e:
                print('==>==>[INFO] El numero de opcion que escribiste no es correcto')
        print('==>[INFO] cargando Archivos...')
        self.df_export=pd.read_excel(os.path.join(NAME_FOLDER_OUTPUTS, self._ficheros[self._numberOption-1],'Formato_2.11_'+self._ficheros[self._numberOption-1]+'_export.xlsx'))
        self.df_truck = pd.read_excel(NAME_DF_DATA_TRUCK)
        self.df_client = pd.read_excel(NAME_DF_DATA_CLIENT)
        self.df_sku_data = pd.read_excel(NAME_DF_SKU_DATA)
        self.df_weight_volume = pd.read_excel(NAME_DF_DATA_WEIGHT_VOLUME)
        self.df_weight_volume['Volumen']=self.df_weight_volume['Volumen'].apply(lambda x:round(x,2))
        self.df_cmi = pd.read_excel(NAME_DF_DATA_CMI)
    def getDirectories(self):
        return self._ficheros
    def getNumberOption(self):
        return self._numberOption