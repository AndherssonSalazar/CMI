import Inputs
import pandas as pd
import copy
class Process:
    def __init__(self,inputs : Inputs) -> None:
        print('==>[INFO] Agrupando por sucursal')
        self.__df_export_order = self._order_by(inputs.df_export,'Sucursal', True)
        self.__df_export_order["Volumen"], self.__df_export_order["Peso"], self.__df_export_order["DOH"], self.__df_export_order["DOHMas"], self.__df_export_order["VolumenTotal"], self.__df_export_order["PesoTotal"]=[0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
        self.__branchs=self.__df_export_order.groupby('Sucursal')
        print('==>[INFO] Obteniendo Las sucursales')
        self.__branchs=pd.DataFrame(self.__branchs.size().reset_index(name = "Cantidad"))
        self.__branchs["Volumen"], self.__branchs["Peso"], self.__branchs["Camion"], self.__branchs["DiferenciaVolumen"], self.__branchs["DiferenciaPeso"] = [0.0, 0.0, '', 0.0, 0.0]
        print('==>[INFO] Obteniendo Peso y volumen Total por sucursal')
        self._volume_weight_x_branch(self.__df_export_order, self.__branchs, inputs.df_weight_volume)
        self.__list_branchs_consolidation=copy.deepcopy(self.__branchs)
        print('==>[INFO] Consolidando rutas')
        self.__list_branchs_consolidation=self._consolidation_routes(self.__list_branchs_consolidation, inputs.df_cmi)
        print('==>[INFO] Asignando camiones mas optimos')
        self.__list_branchs_consolidation[1]=self._verify_match_truck(self.__list_branchs_consolidation[1], inputs.df_truck)
        print('==>[INFO] Hallando la diferencia total(Volumen, Peso) del camion vs consolidado')
        self.__list_branchs_consolidation[1]=self._difference_truck_branch(inputs.df_truck, self.__list_branchs_consolidation[1])
        print('==>[INFO] Asignando la diferencia(volumen, peso) faltante a cada sucursal')
        self.__list_branchs_consolidation[0]["DOH"], self.__list_branchs_consolidation[0]["DOHMas"]=[0.0, 0.0]
        self.__list_branchs_consolidation[0]=self._assign_weight_volume_difference_to_branch(self.__list_branchs_consolidation[0], self.__list_branchs_consolidation[1])
        self.__list_branchs_consolidation[1]=self.__list_branchs_consolidation[1].rename (columns = {'Sucursal':'Consolidado'})
        del self.__list_branchs_consolidation[0]['Camion']
        self.__list_branchs_consolidation[0]["Picking"], self.__list_branchs_consolidation[0]["%Picking"]=[0.0, 0.0]
        print('==>[INFO] Haciendo Match DOH con las sucursales')
        self.__list_branchs_consolidation[0]=self._match_doht(self.__list_branchs_consolidation[0], inputs.df_client)
        print('==>[INFO] Agrupar Disponible')
        self.__list_availables=self._group_available(self.__df_export_order)
        print('==>[INFO] Ordenar Codigos(A-C) - precios(menor-mayor), volumen(mayor-menor) por cada grupo disponible')
        self.__df_export_order = self._order_by_branch_coment_cod_price_volume_weight(self.__df_export_order)
        for i in range(len(self.__list_availables)):
            self.__list_availables[i]=self._order_by_cod_price_volume_weight(self.__list_availables[i])
        print('==>[INFO] Asignando Dias de Invetario en el orden Establecido')
        for i in range(len(self.__list_availables)):
            print('==>==>[INFO] Asignando a la Sucursal: '+ list(self.__list_availables[i]['Sucursal'])[0])
            self.__list_availables[i]=self._assigning_doh(self.__list_availables[i], self.__list_branchs_consolidation[0])
    def _order_by(self, df, name_of_column, order):
        return df.sort_values(by=[name_of_column], ascending=[order])
    def _order_by_cod_price_volume_weight(self, df):
        return df.sort_values(by=['ABC XYZ', 'PRECIO GIV', 'Volumen', 'Peso'],ascending=[True, True, False, False])
    def _order_by_branch_coment_cod_price_volume_weight(self, df):
        return df.sort_values(by=['Sucursal', 'Comentario', 'ABC XYZ', 'PRECIO GIV', 'Volumen', 'Peso'],ascending=[True, True, True, True, False, False])
    def _volume_weight_x_branch(self, eans, branchs, volumes):
        for ean in eans.itertuples(index=True, name='PandasEANS'):
            found=False
            for vol in volumes.itertuples(index=True, name='PandasVolPes'):
                if ean.EAN==vol.EAN:
                    for bran in branchs.itertuples(index=True, name='PandasSucursales'):
                        if bran.Sucursal==ean.Sucursal:
                            branchs.loc[bran.Index, 'Volumen'] += vol.Volumen
                            branchs.loc[bran.Index, 'Peso'] += vol.Peso
                            found=True
                            break
                if found:
                    eans.loc[ean.Index, 'Volumen']=vol.Volumen
                    eans.loc[ean.Index, 'Peso']=vol.Peso
                    break
    def _consolidation_routes(self, branchs, routes):
        list_consolidation = []
        list_consolidation.append(branchs)
        can_consolidate=pd.DataFrame(routes.iloc[16:])[['CMI REPLENISHMENT MODEL','Unnamed: 1']].rename (columns = {'CMI REPLENISHMENT MODEL':'Sucursal', 'Unnamed: 1':'Ruta'})
        for co in can_consolidate.itertuples(index=True, name='Pandas'):
            can_consolidate.loc[co.Index, 'Sucursal']=co.Sucursal.replace("Dijisa ", '').replace("AGA ", '').replace("Moran ", '').replace("Digumisac ", '').replace("DEL PRADO - ", '').upper()
        group_routes=can_consolidate.groupby('Ruta')
        group_routes=pd.DataFrame(group_routes.size().reset_index(name = "Grupos"))
        branchs_consolidation=None
        for gr in group_routes.itertuples(index=True, name='PandasRoutes'):
            names_co=[]
            amount_co=0
            volume_co=0.0
            peso_co=0.0
            for co in can_consolidate.itertuples(index=True, name='PandasBranchs'):
                if gr.Ruta==co.Ruta:
                    names_co.append(''+co.Sucursal)
                    amount_co +=branchs['Cantidad'][branchs[branchs['Sucursal'] == co.Sucursal].index.values[0]]
                    volume_co +=branchs['Volumen'][branchs[branchs['Sucursal'] == co.Sucursal].index.values[0]]
                    peso_co +=branchs['Peso'][branchs[branchs['Sucursal'] == co.Sucursal].index.values[0]]
            if branchs_consolidation is None:
                branchs_consolidation=pd.DataFrame({'Sucursal': [names_co],
                         'Cantidad': [amount_co],
                         'Volumen': [volume_co],
                         'Peso': [peso_co],
                         'Camion' : [''],
                         'DiferenciaVolumen' : [0.0],
                         'DiferenciaPeso' : [0.0]})
            else:
                branchs_consolidation = pd.concat([branchs_consolidation, pd.DataFrame({'Sucursal': [names_co],
                             'Cantidad': [amount_co],
                             'Volumen': [volume_co],
                             'Peso': [peso_co],
                             'Camion' : [''],
                             'DiferenciaVolumen' : [0.0],
                             'DiferenciaPeso' : [0.0]})], ignore_index = True)
        for name_branch in branchs.itertuples(index=True, name='PandasBranchsO'):
            if not self._exists_in_consolidate(name_branch.Sucursal, branchs_consolidation):
                branchs_consolidation = pd.concat([branchs_consolidation, pd.DataFrame({'Sucursal': [[name_branch.Sucursal]],
                             'Cantidad': [name_branch.Cantidad],
                             'Volumen': [name_branch.Volumen],
                             'Peso': [name_branch.Peso],
                             'Camion' : [''],
                             'DiferenciaVolumen' : [0.0],
                             'DiferenciaPeso' : [0.0]})], ignore_index = True)
        list_consolidation.append(branchs_consolidation)
        return list_consolidation
    def _exists_in_consolidate(self, name, branchs_consolidation):
        for branchs in branchs_consolidation['Sucursal']:
            for name_branch in branchs:
                if name==name_branch:
                    return True
        return False
    def _verify_match_truck(self, branchs, trucks):
        trucks_order=self._order_by(trucks,'Volumen Gramel', False).rename (columns = {'Volumen Gramel':'Volumen'})
        branchs_order=self._order_by(branchs,'Volumen', False)
        for bran in branchs_order.itertuples(index=True, name='PandasBranchs'):
            for truck in trucks_order.itertuples(index=True, name='PandasTruck'):
                if truck.Volumen>=bran.Volumen and truck.Peso>=bran.Peso:
                    branchs_order.loc[bran.Index, 'Camion'] = truck.TIPO
                else:
                    break
        return branchs_order
    def _difference_truck_branch(self, trucks, branchs):
        trucks = trucks.rename (columns = {'Volumen Gramel':'Volumen'})
        for bran in branchs.itertuples(index=True, name='PandasBranchs'):
            for truck in trucks.itertuples(index=True, name='PandasTruck'):
                if bran.Camion==truck.TIPO:
                    branchs.loc[bran.Index, 'DiferenciaVolumen']=truck.Volumen-bran.Volumen
                    branchs.loc[bran.Index, 'DiferenciaPeso']=truck.Peso-bran.Peso
                    break
        return branchs
    def _assign_weight_volume_difference_to_branch(self, branchs, branchs_consolidation):
        for consolidation in branchs_consolidation.itertuples(index=True, name='PandasConsolidation'):
            list_names=consolidation.Sucursal
            for name in list_names:
                for branch in branchs.itertuples(index=True, name='PandasBranch'):
                    if branch.Sucursal==name:
                        branchs.loc[branch.Index, 'DiferenciaVolumen']=branch.Volumen/consolidation.Volumen*consolidation.DiferenciaVolumen
                        branchs.loc[branch.Index, 'DiferenciaPeso']=branch.Peso/consolidation.Peso*consolidation.DiferenciaPeso
        return branchs
    def _match_doht(self, branchs, data_client):
        for client in data_client.itertuples(index=True, name='PandasClient'):
            data_client.loc[client.Index, 'Sucursal']=client.Sucursal.replace("Dijisa ", '').replace("AGA ", '').replace("Moran ", '').replace("Digumisac ", '').replace("DEL PRADO - ", '').upper()
        for client in data_client.itertuples(index=True, name='PandasClient'):
            for bran in branchs.itertuples(index=True, name='PandasBranchs'):
                if bran.Sucursal==client.Sucursal:
                    branchs.loc[bran.Index, 'DOH']=client._7
                    branchs.loc[bran.Index, 'DOHMas']=client._8
        return branchs
    def _group_available(self, df):
        list_availables=[]
        df_branchs=df.groupby('Sucursal')
        for branch_name in df_branchs.groups.keys():
            branch=df_branchs.get_group(branch_name)
            list_availables.append(branch.groupby('Comentario').get_group('DISPONIBLE'))
        return list_availables
    def _assigning_doh(self, df, branchs):
        for product in df.itertuples(index=True, name='PandasProducts'):
            if product._16!='CX' or product._16!='CY' or product._16!='CZ':
                for branch in branchs.itertuples(index=True, name='PandasBranchs'):
                    if product.Sucursal==branch.Sucursal:
                        df.loc[product.Index, 'DOH']=branch.DOH
        return df
    def get_data(self):
        return self.__df_export_order
    def get_branchs_data(self):
        return self.__list_branchs_consolidation[0]
    def get_trucks_consolidate(self):
        return self.__list_branchs_consolidation[1]