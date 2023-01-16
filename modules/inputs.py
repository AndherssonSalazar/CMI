from math import prod
import os
import pandas as pd
import pathlib
import warnings
from modules.params import(
    NAME_DF_AGA,
    NAME_DF_AB,
    NAME_DF_DIJISA,
    NAME_DF_VEGA,
    NAME_FOLDER_OUTPUTS,
    NAME_DF_DATA_TRUCK,
    NAME_DF_DATA_CLIENT,
    NAME_DF_SKU_DATA,
    NAME_DF_DATA_WEIGHT_VOLUME,
    NAME_DF_DATA_CMI,
    NAME_DF_DATA_MOQ,
    NAME_GRUPO_VEGA
)
class Inputs:
    def __init__(self) -> None:
        self.isAutomatic=True
        self.is_grupo_vega = False
        print('==>[INFO] ELija una opcion')
        warnings.simplefilter("ignore")
        if self.isAutomatic:
            self._getAutomaticOutputs()
        else:
            self._getManualOutputs()
        self.df_truck = pd.read_excel(NAME_DF_DATA_TRUCK, engine="openpyxl")
        self.df_client = pd.read_excel(NAME_DF_DATA_CLIENT, engine="openpyxl")
        self.df_sku_data = pd.read_excel(NAME_DF_SKU_DATA, engine="openpyxl")
        self.df_weight_volume = pd.read_excel(NAME_DF_DATA_WEIGHT_VOLUME, engine="openpyxl")
        #self.df_weight_volume['Volumen']=self.df_weight_volume['Volumen'].apply(lambda x:round(x,2))
        self.df_cmi = pd.read_excel(NAME_DF_DATA_CMI, usecols="A,B,C")
        if not self.isAutomatic and self.is_grupo_vega :
            self.df_moq = pd.read_excel(NAME_DF_DATA_MOQ, engine="openpyxl")
    def _getAutomaticOutputs(self):
        directorio = pathlib.Path(NAME_FOLDER_OUTPUTS)
        self._ficheros = [fichero.name for fichero in directorio.iterdir() if not fichero.is_file()]
        if len(self._ficheros)<=0:
            raise ValueError("[Error]======No existen carpetas dentro de Outputs ======[Error]")
        for i in range(len(self._ficheros)):
            print("==>==> ["+str(i+1)+"]. "+self._ficheros[i])


        self._numberOption=0
        incorrect=True
        while incorrect:
            try:
                self._numberOption=int(input('==>==>[INPUT] Escribe el numero de la sucursal por favor: '))
                if self._numberOption>0 and self._numberOption<=len(self._ficheros):
                    incorrect=False
                    if self._ficheros[self._numberOption] == NAME_GRUPO_VEGA:
                        self.is_grupo_vega = True
                else:
                    print('==>==>[INFO] El numero de opcion que escribiste no es correcto')
            except Exception as e:
                print('==>==>[INFO] El numero de opcion que escribiste no es correcto')
        print('==>[INFO] cargando Archivos...')
        file = self._get_file()
        #self.df_export=pd.read_excel(
        #   os.path.join(
        #       NAME_FOLDER_OUTPUTS,
        #       self._ficheros[self._numberOption-1],
        #       'Formato_2.11_'+self._ficheros[self._numberOption-1]+'_export.xlsx'),
        #   engine="openpyxl")
        print('Filename: ',file)
        self.df_export=pd.read_excel(file, engine="openpyxl")

    def _get_file(self):
        directorio = pathlib.Path(os.path.join(NAME_FOLDER_OUTPUTS,self._ficheros[self._numberOption-1]))
        files = [f.name for f in directorio.iterdir() if f.is_file()]
        if len(files)>1:
            print('==>==>[ERROR] La cantidad de archivos dentro de la carpeta del cliente no es el correcto')
            raise Exception('==>==>[ERROR] La cantidad de archivos dentro de la carpeta del cliente no es el correcto')
        return os.path.join(directorio , files[0])
        

    def _getManualOutputs(self):
        print("==>==> [1]. AGA")
        print("==>==> [2]. Alvarez Bohl")
        print("==>==> [3]. Dijisa")
        print("==>==> [4]. Grupo Vega")
        self._numberOption=0
        incorrect=True
        while incorrect:
            try:
                self._numberOption=int(input('==>==>[INPUT] Escribe el numero de la sucursal por favor:: '))
                if self._numberOption>0 and self._numberOption<=4:
                    incorrect=False
                else:
                    print('==>==>[INFO] El numero de opcion que escribiste no es correcto')
            except Exception as e:
                print('==>==>[INFO] El numero de opcion que escribiste no es correcto')
        print('==>[INFO] cargando Archivos...')
        if self._numberOption==1:
            self.df_export=pd.read_excel(os.path.join(NAME_FOLDER_OUTPUTS, NAME_DF_AGA), engine="openpyxl")
        elif self._numberOption==2:
            self.df_export=pd.read_excel(os.path.join(NAME_FOLDER_OUTPUTS, NAME_DF_AB), engine="openpyxl")
        elif self._numberOption==3:
            self.df_export=pd.read_excel(os.path.join(NAME_FOLDER_OUTPUTS, NAME_DF_DIJISA), engine="openpyxl")
        elif self._numberOption==4:
            self.df_export=pd.read_excel(os.path.join(NAME_FOLDER_OUTPUTS, NAME_DF_VEGA), engine="openpyxl")
    def getDirectories(self):
        return self._ficheros
    def getNumberOption(self):
        return self._numberOption