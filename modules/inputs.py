import pandas as pd
from modules.params import(
    NAME_DF_DATA_EXPORT,
    NAME_DF_DATA_EXPORT_BRANCH,
    NAME_DF_DATA_TRUCK,
    NAME_DF_DATA_CLIENT,
    NAME_DF_SKU_DATA,
    NAME_DF_DATA_WEIGHT_VOLUME,
    NAME_DF_DATA_CMI
)
class Inputs:
    def __init__(self) -> None:
        try:
            self.df_export = pd.read_excel(NAME_DF_DATA_EXPORT)
        except Exception as e:
            try:
                self.df_export = pd.read_excel(NAME_DF_DATA_EXPORT1)
            except Exception as e:
                try:
                    self.df_export = pd.read_excel(NAME_DF_DATA_EXPORT2)
                except Exception as e:
                    error_value="[Error]======No existe ninguno de los tres Archivos input ======[Error]"
                    raise ValueError(error_value)
        #self.df_export_branch = pd.read_excel(NAME_DF_DATA_EXPORT_BRANCH)
        self.df_truck = pd.read_excel(NAME_DF_DATA_TRUCK)
        self.df_client = pd.read_excel(NAME_DF_DATA_CLIENT)
        self.df_sku_data = pd.read_excel(NAME_DF_SKU_DATA)
        self.df_weight_volume = pd.read_excel(NAME_DF_DATA_WEIGHT_VOLUME)
        self.df_cmi = pd.read_excel(NAME_DF_DATA_CMI)