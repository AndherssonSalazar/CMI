from numpy import NaN, true_divide
from .inputs import Inputs
from .report import Report
import pandas as pd
import copy
import math
class Process:
    def __init__(self,inputs : Inputs) -> None:
        self._inputs=inputs
        self.__errores=None
        print('==>[INFO] Agrupando por sucursal')
        self.__df_export_order = self._order_by(inputs.df_export,'Sucursal', True)
        print('==>[INFO] Obteniendo Las sucursales')
        self.__branchs=self.__df_export_order.groupby('Sucursal')
        self.__branchs=pd.DataFrame(self.__branchs.size().reset_index(name = "Cantidad"))
        self.__branchs["Ruta"], self.__branchs["Volumen"], self.__branchs["DiferenciaVolumen"], self.__branchs["VolumenAumentado"], self.__branchs["Camion"], self.__branchs["DOHInicial"], self.__branchs["DOHFinal"], self.__branchs["CompraFinal"], self.__branchs["VolumenFinal"], self.__branchs["NCajasPicking"] = ['', 0.0, 0.0, 0.0, '', 0.0, 0.0, 0.0, 0.0, 0.0]
        if not self.is_automatic() and self._inputs._numberOption==4:
            self.__df_export_order["Volumen"], self.__df_export_order["MOQ"], self.__df_export_order["Amarre"], self.__df_export_order["AmarreCama"], self.__df_export_order["VolumenMOQ"], self.__df_export_order["Ajuste"], self.__df_export_order["NumeroMOQs"], self.__df_export_order["NumeroMOQCeil"], self.__df_export_order["NumeroMOQPurchase"], self.__df_export_order["MOQAjustadoFinal"], self.__df_export_order["NCajasAumentar"], self.__df_export_order["VolumenFinal"], self.__df_export_order["CajasPicking"], self.__df_export_order["CompraFinal"], self.__df_export_order["VolumenFinalTotal"]=[0.0, "", 0.0, 0.0, 0.0, 0.0, 0, 0, 0, 0, 0, 0.0, 0, 0, 0.0]
            print('==>[INFO] Obteniendo Peso y volumen Total por Ean')
            self._volume_x_ean(self.__df_export_order, None, inputs.df_weight_volume)
            self.__branchs_other=copy.deepcopy(self.__branchs)
            self.__branchs['Sucursal']=self.__branchs['Sucursal'].apply(lambda x:x+" - Home Care")
            self.__branchs_other['Sucursal']=self.__branchs_other['Sucursal'].apply(lambda x:x+" - Others")
            self.__branchs=pd.concat([self.__branchs, self.__branchs_other], ignore_index = True)
            print('==>[INFO] Obteniendo MOQ por Ean')
            self._moq_x_ean(self.__df_export_order, inputs.df_moq)
            print('==>[INFO] Obteniendo Peso y volumen Total por Sucursal')
            self._volume_x_branch_vega(self.__df_export_order, self.__branchs)
            print('==>[INFO] Obteniendo Amarre por Ean')
            self._amarre_x_ean(self.__df_export_order, inputs.df_sku_data)
            if self.__errores is not None:
                report = Report(None)
                report.save_error(self.__errores)
                raise ValueError("[Error]======Revisar el Reporte de Errores ======[Error]")
            print('==>[INFO] Asignando camiones mas optimos')
            self._match_truck(self.__branchs, inputs.df_truck)
            print('==>[INFO] Separando Home Care y Otros')
            self.__home_care=self._get_home_care(self.__df_export_order)
            self.__others=self._get_others(self.__df_export_order)
            print('==>[INFO] Agrupando por sucursal Home Care')
            self.__list_availables=self._group_available(self.__home_care)
            print('==>[INFO] Ordenar Codigos(A-C) - precios(menor-mayor), volumen(mayor-menor) por cada grupo disponible Home Care')
            for i in range(len(self.__list_availables)):
                self.__list_availables[i]=self._order_by_price_volume(self.__list_availables[i])
            print('==>[INFO] procesando data Home Care')
            for i in range(len(self.__list_availables)):
                print('==>==>[INFO] Asignando o restando volumen Home Care a la Sucursal: '+ list(self.__list_availables[i]['Sucursal'])[0])
                self._process_data_vega(self.__df_export_order, self.__list_availables[i], self.__branchs)
            print('==>[INFO] Agrupando por sucursal Others')
            self.__list_availables=self._group_available(self.__others)
            print('==>[INFO] Ordenar Codigos(A-C) - precios(menor-mayor), volumen(mayor-menor) por cada grupo disponible Others')
            for i in range(len(self.__list_availables)):
                self.__list_availables[i]=self._order_by_price_volume(self.__list_availables[i])
            print('==>[INFO] procesando data Others')
            for i in range(len(self.__list_availables)):
                print('==>==>[INFO] Asignando o restando volumen Others a la Sucursal: '+ list(self.__list_availables[i]['Sucursal'])[0])
                self._process_data_vega(self.__df_export_order, self.__list_availables[i], self.__branchs)
            print('==>[INFO] Llenado de data producto')
            self._fill_data_product_vega(self.__df_export_order)
            print('==>[INFO] Llenado de data reporte Sucursal')
            self._fill_data_branch_vega(self.__df_export_order, self.__branchs)
        else:
            self.__df_export_order["Volumen"], self.__df_export_order["Amarre"], self.__df_export_order["VolumenAjustado"], self.__df_export_order["NCajasAumentar"], self.__df_export_order["NCajasAumentarCeil"], self.__df_export_order["VolumenAumentarDisminuir"], self.__df_export_order["FinalPurchaseFinal"], self.__df_export_order["VolumenFinal"], self.__df_export_order["PorcentajePallet"], self.__df_export_order["AjustePallet"], self.__df_export_order["VolumenFinalTotal"], self.__df_export_order["NCajasPicking"]=[0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
            print('==>[INFO] Obteniendo Peso y volumen Total por Ean y sucursal')
            self._volume_x_ean(self.__df_export_order, self.__branchs, inputs.df_weight_volume)
            print('==>[INFO] Consolidando rutas')
            self.__branchs_consolidation=copy.deepcopy(self.__branchs).rename (columns = {'Sucursal':'Consolidado'})
            self.__branchs_consolidation=self._consolidation_routes(self.__branchs_consolidation, inputs.df_cmi)
            print('==>[INFO] Asignando camiones mas optimos')
            self.__branchs_consolidation=self._verify_match_truck(self.__branchs_consolidation, inputs.df_truck)
            print('==>[INFO] Asignando la diferencia de volumen faltante a cada sucursal')
            self._assign_volume_difference_to_branch(self.__branchs, self.__branchs_consolidation)
            print('==>[INFO] Match data cliente')
            self._match_data_client(self.__branchs, inputs.df_client)
            print('==>[INFO] Obteniendo Amarre por Ean')
            self._amarre_x_ean(self.__df_export_order, inputs.df_sku_data)
            if self.__errores is not None:
                report = Report(None)
                report.save_error(self.__errores)
                raise ValueError("[Error]======Revisar el Reporte de Errores ======[Error]")
            print('==>[INFO] Agrupar Disponible')
            self.__list_availables=self._group_available(self.__df_export_order)
            print('==>[INFO] Ordenar Codigos(A-C) - precios(menor-mayor), volumen(mayor-menor) por cada grupo disponible')
            #self.__df_export_order = self._order_by_branch_coment_cod_price_volume(self.__df_export_order)
            for i in range(len(self.__list_availables)):
                self.__list_availables[i]=self._order_by_price_volume(self.__list_availables[i])
            print('==>[INFO] Asignando o quitando volumen para camiones optimos')
            for i in range(len(self.__list_availables)):
                print('==>==>[INFO] Asignando o restando volumen a la Sucursal: '+ list(self.__list_availables[i]['Sucursal'])[0])
                self._assigning_volume_weigth_remaining(self.__df_export_order, self.__list_availables[i], self.__branchs)
            print('==>[INFO] Rutas y Camiones a la sucursal')
            self._fill_route_truck_branch(self.__branchs, self.__branchs_consolidation)
            print('==>[INFO] Llenado de data producto')
            self._fill_data_product(self.__df_export_order)
            print('==>[INFO] Llenado de data reporte Sucursal')
            self._fill_data_branch(self.__df_export_order, self.__branchs)
        print('==>[INFO] Limpiar Campos Innecesarios')
        self._delete_unnecesary_fields()
    def getInputs(self):
        return self._inputs
    def _order_by(self, df, name_of_column, order):
        return df.sort_values(by=[name_of_column], ascending=[order])
    def _order_by_cod_price_volume(self, df):
        return df.sort_values(by=['ABC XYZ', 'PRECIO GIV', 'Volumen'],ascending=[True, False, False])
    def _order_by_price_volume(self, df):
        return df.sort_values(by=['PRECIO GIV', 'Volumen'],ascending=[True, False])
    def _order_by_branch_coment_cod_price_volume(self, df):
        return df.sort_values(by=['Sucursal', 'Comentario', 'ABC XYZ', 'PRECIO GIV', 'Volumen'],ascending=[True, True, True, False, False])
    def _volume_x_ean(self, eans, branchs, volumes):
        sinVolumenPeso=[]
        volumenCero=[]
        for ean in eans.itertuples(index=True, name='PandasEANS'):
            encontrado=False
            if ean.Comentario=='DESCONTINUADO': continue
            for vol in volumes.itertuples(index=True, name='PandasVolPes'):
                if ean.EAN==vol.EAN:
                    encontrado=True
                    if vol.Volumen<=0.0 or vol.Volumen!=vol.Volumen:
                        exist=False
                        for i in range(len(volumenCero)):
                            if volumenCero[i]["EAN"]==ean.EAN:
                                exist=True
                                break
                        if not exist:
                            volumenCero.append({"EAN":ean.EAN, "Descripcion":ean.Descripción})
                    else:
                        eans.loc[ean.Index, 'Volumen'] = vol.Volumen
                    break
            if not encontrado:
                exists=False
                for i in range(len(sinVolumenPeso)):
                    if sinVolumenPeso[i]["EAN"]==ean.EAN:
                        exists=True
                        break
                if not exists:
                    sinVolumenPeso.append({"EAN":ean.EAN, "Descripcion":ean.Descripción})
        if len(sinVolumenPeso)>0:
            ErroresEANSVol = pd.DataFrame(sinVolumenPeso)
            ErroresEANSVol.loc[:,'Comentario'] = 'Sin Volumen'
            self.__errores=ErroresEANSVol
        if len(volumenCero)>0:
            ErroresEANSVolu = pd.DataFrame(volumenCero)
            ErroresEANSVolu.loc[:,'Comentario'] = 'Volumen de EAN "no Descontinuado" no puede ser igual o menor que cero'
            self.__errores= pd.concat([self.__errores, ErroresEANSVolu], ignore_index = True)
        if branchs is not None:
            self._volume_weight_x_branch(eans, branchs)
    def _volume_weight_x_branch(self, eans, branchs):
        # ean._9 = "Final Purchase"
        FinalPurchaseNaN=[]
        eansTemp=eans.rename(columns = {'Final Purchase':'FinalPurchase'})
        for ean in eansTemp.itertuples(index=True, name='PandasEANS'):
            if ean.Comentario=='DESCONTINUADO': continue
            for bran in branchs.itertuples(index=True, name='PandasSucursales'):
                if ean.Sucursal.upper()==bran.Sucursal.upper():
                    if ean.FinalPurchase!=ean.FinalPurchase:
                        FinalPurchaseNaN.append({"EAN":ean.EAN, "Descripcion":ean.Descripción})
                    branchs.loc[bran.Index, 'Volumen'] += ean.Volumen * ean.FinalPurchase
                    break
        if len(FinalPurchaseNaN)>0:
            ErroresEANSVolu = pd.DataFrame(FinalPurchaseNaN)
            ErroresEANSVolu.loc[:,'Comentario'] = 'Final Purchase Desconocido'
            self.__errores= pd.concat([self.__errores, ErroresEANSVolu], ignore_index = True)
    def _amarre_x_ean(self, df, skus):
        # sk._6 = "Amarre Camas"
        skusTemp=skus.rename(columns = {'AMARRE CAMAS':'AMARRECAMAS'})
        sinAmarre=[]
        for ean in df.itertuples(index=True, name='PandasEANS'):
            encontrado=False
            if ean.Comentario=='DESCONTINUADO': continue
            for sk in skusTemp.itertuples(index=True, name='PandasSKUS'):
                if ean.EAN==sk.EAN:
                    encontrado=True
                    if sk.AMARRE<=0.0 or sk.AMARRE!=sk.AMARRE or sk.AMARRECAMAS<=0.0 or sk.AMARRECAMAS!=sk.AMARRECAMAS:
                        exist=False
                        for i in range(len(sinAmarre)):
                            if sinAmarre[i]["EAN"]==ean.EAN:
                                exist=True
                                break
                        if not exist:
                            sinAmarre.append({"EAN":ean.EAN, "Descripcion":ean.Descripción})
                    else:
                        df.loc[ean.Index, 'Amarre'] = sk.AMARRE
                        df.loc[ean.Index, 'AmarreCama']=sk.AMARRECAMAS
                    break
            if not encontrado:
                #df.loc[ean.Index, 'Amarre'] = 1
                exists=False
                for i in range(len(sinAmarre)):
                    if sinAmarre[i]["EAN"]==ean.EAN:
                        exists=True
                        break
                if not exists:
                    sinAmarre.append({"EAN":ean.EAN, "Descripcion":ean.Descripción})
        if len(sinAmarre)>0:
            ErroresEANS = pd.DataFrame(sinAmarre)
            ErroresEANS.loc[:,'Comentario'] = 'Sin Amarre'
            self.__errores= pd.concat([self.__errores, ErroresEANS], ignore_index = True)
    def _consolidation_routes(self, branchs, routes):
        can_consolidate=pd.DataFrame(routes.iloc[21:])[['CMI REPLENISHMENT MODEL','Unnamed: 1','Unnamed: 2']].rename(columns = {'CMI REPLENISHMENT MODEL':'Sucursal', 'Unnamed: 1':'Ruta', 'Unnamed: 2':'TipoCamion'})
        can_consolidate=can_consolidate.reset_index()
        ind=-1
        for co in can_consolidate.itertuples(index=True, name='Pandas'):
            if type(co.Sucursal)!=str:
                ind=co.Index
                break
        if ind!=-1:
            numberRows=len(can_consolidate)-ind
            for i in range(numberRows):
                can_consolidate=can_consolidate.drop(ind)
                ind+=1
        group_routes=can_consolidate.groupby('Ruta')
        group_routes=pd.DataFrame(group_routes.size().reset_index(name = "Grupos"))
        branchs_consolidation=None
        for gr in group_routes.itertuples(index=True, name='PandasRoutes'):
            names_co=[]
            amount_co=0
            volume_co=0.0
            tipo_co=0
            ruta=gr.Ruta
            for co in can_consolidate.itertuples(index=True, name='PandasConsolidates'):
                if gr.Ruta==co.Ruta:
                    for bra in branchs.itertuples(index=True, name='PandasBranchs'):
                        if co.Sucursal.upper()==bra.Consolidado.upper():
                            names_co.append(''+co.Sucursal)
                            amount_co +=bra.Cantidad
                            volume_co +=bra.Volumen
                            tipo_co=co.TipoCamion
                            break
            if branchs_consolidation is None:
                branchs_consolidation=pd.DataFrame({'Consolidado': [names_co],
                         'Cantidad': amount_co,
                         'Volumen': volume_co,
                         'TipoCamion':tipo_co,
                         'Ruta':ruta})
            else:
                branchs_consolidation = pd.concat([branchs_consolidation, pd.DataFrame({'Consolidado': [names_co],
                             'Cantidad': amount_co,
                             'Volumen': volume_co,
                             'TipoCamion':tipo_co,
                             'Ruta':ruta})], ignore_index = True)
        for name_branch in branchs.itertuples(index=True, name='PandasBranchsO'):
            if not self._exists_in_consolidate(name_branch.Consolidado, branchs_consolidation):
                error_value="[Error]======No existe la ruta para consolidar la sucursal: "+ name_branch.Consolidado+' ======[Error]'
                raise ValueError(error_value)
        return branchs_consolidation
    def _exists_in_consolidate(self, name, branchs_consolidation):
        for branchs in branchs_consolidation['Consolidado']:
            for name_branch in branchs:
                if name.upper()==name_branch.upper():
                    return True
        return False
    def _verify_match_truck(self, consolidation, trucks):
        trucks_order=(self._order_by(trucks,'Volumen Gramel', False)).rename (columns = {'Volumen Gramel':'Volumen', 'LTL?':'LTL'})
        for bran in consolidation.itertuples(index=True, name='PandasBranchs'):
            volumeDifference=float('inf')
            listTrucks=[]
            volumeDifference=self._get_list_of_trucks(volumeDifference, listTrucks, trucks_order, bran)
            list_trucks=[]
            for i in range(len(listTrucks)):
                list_trucks.append(listTrucks[i].TIPO)
            consolidation.loc[bran.Index, 'Camion'] = str(list_trucks)
            consolidation.loc[bran.Index, 'DiferenciaVolumen']=volumeDifference
        return consolidation
    def _get_list_of_trucks(self, volumeDifference, listTrucks, trucks, branch):
        accumulateVolume=0.0
        listTrucksTemp=[]
        volumeDifferenceTemp=volumeDifference
        listTrucksTempChange=[]
        volumeDifferenceTempChange=volumeDifference
        for i in range(len(listTrucks)):
            accumulateVolume+=listTrucks[i].Volumen
            listTrucksTemp.append(listTrucks[i])
            listTrucksTempChange.append(listTrucks[i])
        for truck in trucks.itertuples(index=True, name='PandasTruck'):
            if math.fabs(truck.Volumen+accumulateVolume-branch.Volumen)<math.fabs(volumeDifferenceTempChange) and self._verify_LTL(branch.TipoCamion, truck):
                listTrucksTempChange.append(truck)
                volumeDifferenceTempChange=truck.Volumen+accumulateVolume-branch.Volumen
                volumeDifferenceTempChange=self._get_list_of_trucks(volumeDifferenceTempChange, listTrucksTempChange, trucks, branch)
                if math.fabs(volumeDifferenceTempChange)<math.fabs(volumeDifferenceTemp):
                    listTrucksTemp.clear()
                    for i in range(len(listTrucksTempChange)):
                        listTrucksTemp.append(listTrucksTempChange[i])
                    volumeDifferenceTemp=volumeDifferenceTempChange
                listTrucksTempChange.clear()
                for i in range(len(listTrucks)):
                    listTrucksTempChange.append(listTrucks[i])
                volumeDifferenceTempChange=volumeDifference
        listTrucks.clear()
        for i in range(len(listTrucksTemp)):
            listTrucks.append(listTrucksTemp[i])
        volumeDifference=volumeDifferenceTemp
        return volumeDifference
    def _verify_LTL(self, numero, truck):
        if numero==1:
            if truck.LTL=='No':
                return True
            else:
                return False
        elif numero==2:
            if truck.LTL=='No' or truck.TIPO=='PE21':
                return True
            else:
                return False
        elif numero==3:
            if truck.LTL=='Si' or truck.LTL=='No':
                return True
            else:
                return False
        else:
            error_value="[Error]====== Solo se acepta estos 3 valores 1:NO LTL, 2:NO LTL-PE21, 3 Ambos ======[Error]"
            raise ValueError(error_value)
    def _assign_volume_difference_to_branch(self, branchs, branchs_consolidation):
        for consolidation in branchs_consolidation.itertuples(index=True, name='PandasConsolidation'):
            list_names=consolidation.Consolidado
            for name in list_names:
                for branch in branchs.itertuples(index=True, name='PandasBranch'):
                    if branch.Sucursal.upper()==name.upper():
                        branchs.loc[branch.Index, 'DiferenciaVolumen']=branch.Volumen/consolidation.Volumen*consolidation.DiferenciaVolumen
    def _match_data_client(self, branchs, data_client):
        # client._11 = "% Picking +"
        # client._12 = "% Picking -"
        data_clientTemp=data_client.rename(columns = {'% Picking +':'PickingMas', '% Picking -':'PickingMenos'})
        for client in data_clientTemp.itertuples(index=True, name='PandasClient'):
            for bran in branchs.itertuples(index=True, name='PandasBranchs'):
                if bran.Sucursal.upper()==client.Sucursal.upper():
                    branchs.loc[bran.Index, 'PickingMas']=client.PickingMas
                    branchs.loc[bran.Index, 'PickingMenos']=client.PickingMenos
    def _group_available(self, df):
        list_availables=[]
        df_branchs=df.groupby('Sucursal')
        for branch_name in df_branchs.groups.keys():
            branch=df_branchs.get_group(branch_name)
            list_availables.append(branch.groupby('Comentario').get_group('DISPONIBLE'))
        return list_availables
    def _assigning_volume_weigth_remaining(self, df, group_df, branchs):
        # product._9 = "Final Purchase"
        # product._16 = "ABC XYZ"
        group_dfTemp=group_df.rename(columns = {'Final Purchase':'FinalPurchase', 'ABC XYZ':'ABCXYZ'})
        totalSumVol=self._getTotalVolumen(group_df)
        for product in group_dfTemp.itertuples(index=True, name='PandasProducts'):
            if product.ABCXYZ=='CX' or product.ABCXYZ=='CY' or product.ABCXYZ=='CZ' or product.Volumen==0 or product.Comentario=='DESCONTINUADO': continue
            for branch in branchs.itertuples(index=True, name='PandasBranchs'):
                if branch.DiferenciaVolumen==0.0: continue
                elif (branch.DiferenciaVolumen - branch.VolumenAumentado)==0.0: continue
                elif product.Sucursal.upper()==branch.Sucursal.upper():
                    VolumenAjustado=((product.FinalPurchase*product.Volumen)/totalSumVol)*branch.DiferenciaVolumen
                    NCajasAumentar=VolumenAjustado/product.Volumen
                    if round(NCajasAumentar)==0: break
                    NCajasAumentarCeil=0
                    VolumenAumentarDisminuir=0.0
                    FinalPurchaseFinal=0.0
                    VolumenFinal=0.0
                    PorcentajePallet=0.0
                    NCajasPicking=0.0
                    AjustePallet=0.0
                    VolumenFinalTotal=0.0
                    numberPurchaseGoal = round(NCajasAumentar) if branch.DiferenciaVolumen>0 else -round(NCajasAumentar)
                    lastVolume=0.0
                    for i in range(numberPurchaseGoal):
                        NCajasAumentarCeil = i+1 if branch.DiferenciaVolumen>0 else -i-1
                        VolumenAumentarDisminuir= NCajasAumentarCeil*product.Volumen
                        FinalPurchaseFinal= NCajasAumentarCeil+product.FinalPurchase
                        VolumenFinal= product.Volumen*product.FinalPurchase+VolumenAumentarDisminuir
                        PorcentajePallet = FinalPurchaseFinal/product.Amarre
                        if PorcentajePallet<1.0:
                            NCajasPicking=PorcentajePallet*product.Amarre
                        else:
                            NCajasPicking=(PorcentajePallet-math.floor(PorcentajePallet))*product.Amarre
                        if branch.DiferenciaVolumen>0.0:
                            if PorcentajePallet<=1.0: #menos a una pallet
                                if PorcentajePallet>=branch.PickingMas:
                                    AjustePallet = 1.0*product.Amarre
                                else:
                                    AjustePallet = PorcentajePallet*product.Amarre
                            else: #mas de una pallet
                                missingPercent = PorcentajePallet-math.floor(PorcentajePallet)
                                if missingPercent<branch.PickingMenos:
                                    AjustePallet = math.floor(PorcentajePallet)*product.Amarre
                                elif missingPercent>=branch.PickingMenos and missingPercent<=branch.PickingMas:
                                    AjustePallet = PorcentajePallet*product.Amarre
                                else:
                                    AjustePallet = (math.floor(PorcentajePallet)+1.0)*product.Amarre
                            VolumenFinalTotal=AjustePallet*product.Volumen
                            if VolumenFinalTotal<product.Volumen*product.FinalPurchase:
                                continue
                            if (VolumenFinalTotal-product.Volumen*product.FinalPurchase+branchs['VolumenAumentado'][branch.Index])<=branch.DiferenciaVolumen:
                                #LLenado de Data
                                df.loc[product.Index, 'VolumenAjustado']=VolumenAjustado
                                df.loc[product.Index, 'NCajasAumentar']=NCajasAumentar
                                df.loc[product.Index, 'NCajasAumentarCeil']=NCajasAumentarCeil
                                df.loc[product.Index, 'VolumenAumentarDisminuir']=VolumenAumentarDisminuir
                                df.loc[product.Index, 'FinalPurchaseFinal']=FinalPurchaseFinal
                                df.loc[product.Index, 'VolumenFinal']=VolumenFinal
                                df.loc[product.Index, 'PorcentajePallet']=PorcentajePallet
                                df.loc[product.Index, 'AjustePallet']=AjustePallet
                                df.loc[product.Index, 'VolumenFinalTotal']=VolumenFinalTotal
                                df.loc[product.Index, 'NCajasPicking']=NCajasPicking
                                branchs.loc[branch.Index, 'VolumenAumentado']+=VolumenFinalTotal-product.Volumen*product.FinalPurchase-lastVolume
                                lastVolume=VolumenFinalTotal-product.Volumen*product.FinalPurchase
                        else:
                            AjustePallet = PorcentajePallet*product.Amarre
                            VolumenFinalTotal=AjustePallet*product.Volumen
                            if VolumenFinalTotal>product.Volumen*product.FinalPurchase:
                                continue
                            if (VolumenFinalTotal-product.Volumen*product.FinalPurchase+branchs['VolumenAumentado'][branch.Index])>=branch.DiferenciaVolumen:
                                #LLenado de Data
                                df.loc[product.Index, 'VolumenAjustado']=VolumenAjustado
                                df.loc[product.Index, 'NCajasAumentar']=NCajasAumentar
                                df.loc[product.Index, 'NCajasAumentarCeil']=NCajasAumentarCeil
                                df.loc[product.Index, 'VolumenAumentarDisminuir']=VolumenAumentarDisminuir
                                df.loc[product.Index, 'FinalPurchaseFinal']=FinalPurchaseFinal
                                df.loc[product.Index, 'VolumenFinal']=VolumenFinal
                                df.loc[product.Index, 'PorcentajePallet']=PorcentajePallet
                                df.loc[product.Index, 'AjustePallet']=AjustePallet
                                df.loc[product.Index, 'VolumenFinalTotal']=VolumenFinalTotal
                                df.loc[product.Index, 'NCajasPicking']=NCajasPicking
                                branchs.loc[branch.Index, 'VolumenAumentado']+=VolumenFinalTotal-product.Volumen*product.FinalPurchase-lastVolume
                                lastVolume=VolumenFinalTotal-product.Volumen*product.FinalPurchase
                    break
    def _getTotalVolumen(self, df_group):
        # product._9 = "Final Purchase"
        # product._16 = "ABC XYZ"
        df_groupTemp=df_group.rename(columns = {'Final Purchase':'FinalPurchase', 'ABC XYZ':'ABCXYZ'})
        sumVol=0.0
        for product in df_groupTemp.itertuples(index=True, name='PandasProducts'):
            if product.ABCXYZ=='CX' or product.ABCXYZ=='CY' or product.ABCXYZ=='CZ' or product.Volumen==0 or product.Comentario=='DESCONTINUADO': continue
            sumVol+=product.Volumen*product.FinalPurchase
        return sumVol
    def _fill_route_truck_branch(self, branchs, branchs_consolidation):
        for consolidation in branchs_consolidation.itertuples(index=True, name='PandasConsolidation'):
            list_names=consolidation.Consolidado
            for name in list_names:
                for branch in branchs.itertuples(index=True, name='PandasBranch'):
                    if branch.Sucursal.upper()==name.upper():
                        branchs.loc[branch.Index, 'Ruta']=consolidation.Ruta
                        branchs.loc[branch.Index, 'Camion']=consolidation.Camion
    def _fill_data_product(self, df):
        # product._9 = "Final Purchase"
        dfTemp=df.rename(columns = {'Final Purchase':'FinalPurchase'})
        for product in dfTemp.itertuples(index=True, name='PandasProducts'):
            if product.Comentario=='DESCONTINUADO': continue
            if product.NCajasAumentarCeil==0.0:
                df.loc[product.Index, 'AjustePallet']=product.FinalPurchase
                df.loc[product.Index, 'VolumenFinalTotal']=product.Volumen*product.FinalPurchase
            ppallet=df['AjustePallet'][product.Index]/product.Amarre
            if ppallet<1.0:
                df.loc[product.Index, 'NCajasPicking']=ppallet*product.Amarre
            else:
                df.loc[product.Index, 'NCajasPicking']=(ppallet-math.floor(ppallet))*product.Amarre
    def _fill_data_branch(self, df, branchs):
        # product._11 = "PRECIO GIV"
        # product._12 = "Venta mensual con factor"
        # product._22 = "Inv + trans"
        dfTemp=df.rename(columns = {'PRECIO GIV':'PRECIOGIV', 'Venta mensual con factor':'VentaMensual', 'Inv + trans':'InvTrans'})
        for branch in branchs.itertuples(index=True, name='PandasBranchs'):
            sumaPicking=0.0
            sumaFinalPurchase=0.0
            sumaInventario=0.0
            sumaVentaMensualFactor=0.0
            sumaInventarioAjuste=0.0
            compraFinal=0.0
            for product in dfTemp.itertuples(index=True, name='PandasProducts'):
                if product.Sucursal.upper()==branch.Sucursal.upper():
                    sumaPicking+=product.NCajasPicking
                    sumaFinalPurchase+=product.AjustePallet
                    sumaInventario+=product.Inventario
                    sumaInventarioAjuste+=(product.InvTrans + product.AjustePallet)
                    sumaVentaMensualFactor+=product.VentaMensual
                    compraFinal+=product.AjustePallet*product.PRECIOGIV
            branchs.loc[branch.Index, 'NCajasPicking']=sumaPicking/sumaFinalPurchase
            branchs.loc[branch.Index, 'DOHInicial']=sumaInventario*30/sumaVentaMensualFactor
            branchs.loc[branch.Index, 'DOHFinal']=sumaInventarioAjuste*30/sumaVentaMensualFactor
            branchs.loc[branch.Index, 'CompraFinal']=compraFinal
            branchs.loc[branch.Index, 'VolumenFinal']=branch.Volumen+branch.VolumenAumentado
    #-------------Grupo Vega----------------
    def _volume_x_branch_vega(self, eans, branchs):
        # ean._12 = "Final Purchase"
        eansTemp=eans.rename(columns = {'Final Purchase':'FinalPurchase'})
        othersMOQ=[]
        for ean in eansTemp.itertuples(index=True, name='PandasEANS'):
            if ean.Comentario=='DESCONTINUADO': continue
            for bran in branchs.itertuples(index=True, name='PandasSucursales'):
                if self._compare_branch(bran, ean):
                    if ean.MOQ.upper()=="CAJAS":
                        branchs.loc[bran.Index, 'Volumen']+= ean.Volumen*ean.FinalPurchase
                    elif ean.MOQ.upper()=="PALLET":
                        branchs.loc[bran.Index, 'Volumen']+= ean.Volumen*ean.FinalPurchase*ean.Amarre
                    elif ean.MOQ.upper()=="CAMAS":
                        branchs.loc[bran.Index, 'Volumen']+= ean.Volumen*ean.FinalPurchase*ean.AmarreCama
                    else:
                        existErrors=False
                        if(self.__errores is not None):
                            for i in range(len(self.__errores)):
                                if self.__errores["EAN"][i]==ean.EAN:
                                    existErrors=True
                                    break
                        if existErrors: continue
                        exists=False
                        for i in range(len(othersMOQ)):
                            if othersMOQ[i]["EAN"]==ean.EAN:
                                exists=True
                                break
                        if not exists:
                            othersMOQ.append({"EAN":ean.EAN, "Descripcion":ean.Descripción})
                    break
        if len(othersMOQ)>0:
            ErroresEANSMOQ = pd.DataFrame(othersMOQ)
            ErroresEANSMOQ.loc[:,'Comentario'] = 'Verificar que el MOQ del EAN sea (CAJAS, PALLET, CAMAS)'
            self.__errores= pd.concat([self.__errores, ErroresEANSMOQ], ignore_index = True)
    def _get_home_care(self, df):
        return df.groupby('Categoria').get_group('HOME CARE')
    def _get_branchs_vega(self, df):
        list_availables=[]
        df_branchs=df.groupby('Sucursal')
        for branch_name in df_branchs.groups.keys():
            list_availables.append(df_branchs.get_group(branch_name))
        return list_availables
    def _get_others(self, df):
        others=None
        df_categories=df.groupby('Categoria')
        for categorie_name in df_categories.groups.keys():
            if categorie_name!='HOME CARE':
                if others is None:
                    others=df.groupby('Categoria').get_group(categorie_name)
                else:
                    others = pd.concat([others, df.groupby('Categoria').get_group(categorie_name)], ignore_index = False)
        return others
    def _moq_x_ean(self, eans, moqs):
        # ean._2 = "Codigo EAN"
        moqsTemp=moqs.rename(columns = {'Codigo EAN':'CodigoEAN'})
        sinMOQ=[]
        for ean in eans.itertuples(index=True, name='PandasEANS'):
            encontrado=False
            if ean.Comentario=='DESCONTINUADO': continue
            for moq in moqsTemp.itertuples(index=True, name='PandasMOQ'):
                if ean.EAN==moq.CodigoEAN:
                    encontrado=True
                    try:
                        eans.loc[ean.Index, 'MOQ'] = moqs[ean.Sucursal.upper()][moq.Index]
                    except Exception as e:
                        error_value="[Error]======Sucursal '"+ean.Sucursal.upper()+"' No existe en el MOQ======[Error]"
                        raise ValueError(error_value)
                    break
            if not encontrado:
                exists=False
                for i in range(len(sinMOQ)):
                    if sinMOQ[i]["EAN"]==ean.EAN:
                        exists=True
                        break
                if not exists:
                    sinMOQ.append({"EAN":ean.EAN, "Descripcion":ean.Descripción})
        if len(sinMOQ)>0:
            ErroresEANSMOQ = pd.DataFrame(sinMOQ)
            ErroresEANSMOQ.loc[:,'Comentario'] = 'Sin MOQ'
            self.__errores= pd.concat([self.__errores, ErroresEANSMOQ], ignore_index = True)
    def _match_truck(self, consolidation, trucks):
        trucks_order=(self._order_by(trucks,'Volumen Gramel', False)).rename (columns = {'Volumen Gramel':'Volumen', 'LTL?':'LTL', 'Min Pallet':'MinPallet', 'Max Pallet':'MaxPallet'})
        for bran in consolidation.itertuples(index=True, name='PandasBranchs'):
            volumeDifference=float('inf')
            listTrucks=[]
            volumeDifference=self._get_list_of_trucks_vega(volumeDifference, listTrucks, trucks_order, bran)
            list_trucks=[]
            for i in range(len(listTrucks)):
                list_trucks.append(listTrucks[i].TIPO)
            consolidation.loc[bran.Index, 'Camion'] = str(list_trucks)
            consolidation.loc[bran.Index, 'DiferenciaVolumen']=volumeDifference
        return consolidation
    def _get_list_of_trucks_vega(self, volumeDifference, listTrucks, trucks, branch):
        accumulateVolume=0.0
        listTrucksTemp=[]
        volumeDifferenceTemp=volumeDifference
        listTrucksTempChange=[]
        volumeDifferenceTempChange=volumeDifference
        for i in range(len(listTrucks)):
            accumulateVolume+=listTrucks[i].Volumen
            listTrucksTemp.append(listTrucks[i])
            listTrucksTempChange.append(listTrucks[i])
        for truck in trucks.itertuples(index=True, name='PandasTruck'):
            if math.fabs(truck.Volumen+accumulateVolume-branch.Volumen)<math.fabs(volumeDifferenceTempChange) and self._verify_if_belong_branch(branch.Sucursal, truck):
                listTrucksTempChange.append(truck)
                volumeDifferenceTempChange=truck.Volumen+accumulateVolume-branch.Volumen
                volumeDifferenceTempChange=self._get_list_of_trucks_vega(volumeDifferenceTempChange, listTrucksTempChange, trucks, branch)
                if math.fabs(volumeDifferenceTempChange)<math.fabs(volumeDifferenceTemp):
                    listTrucksTemp.clear()
                    for i in range(len(listTrucksTempChange)):
                        listTrucksTemp.append(listTrucksTempChange[i])
                    volumeDifferenceTemp=volumeDifferenceTempChange
                listTrucksTempChange.clear()
                for i in range(len(listTrucks)):
                    listTrucksTempChange.append(listTrucks[i])
                volumeDifferenceTempChange=volumeDifference
        listTrucks.clear()
        for i in range(len(listTrucksTemp)):
            listTrucks.append(listTrucksTemp[i])
        volumeDifference=volumeDifferenceTemp
        return volumeDifference
    def _verify_if_belong_branch(self, sucursal_name, truck):
        if type(truck.Sucursal)!=str:
            return False
        elif truck.Sucursal.upper()=="TODOS" or sucursal_name.upper()==(truck.Sucursal.upper()+" - OTHERS") or sucursal_name.upper()==(truck.Sucursal.upper()+" - HOME CARE"):
            return True
        return False
    def _process_data_vega(self, df, group_df, branchs):
        # product._12 = "Final Purchase"
        # product._14 = "ABC XYZ"
        group_dfTemp=group_df.rename(columns = {'Final Purchase':'FinalPurchase', 'ABC XYZ':'ABCXYZ'})
        totalSumVol=self._getTotalVolumenVega(group_df)
        if totalSumVol==0.0: return
        for product in group_dfTemp.itertuples(index=True, name='PandasProducts'):
            if product.ABCXYZ=='CX' or product.ABCXYZ=='CY' or product.ABCXYZ=='CZ' or product.Volumen==0 or product.Comentario=='DESCONTINUADO' or product.FinalPurchase==0: continue
            for branch in branchs.itertuples(index=True, name='PandasBranchs'):
                if branch.DiferenciaVolumen==0.0: continue
                elif (branch.DiferenciaVolumen- branch.VolumenAumentado)==0.0: continue
                elif self._compare_branch(branch, product):
                    VolumenMOQ=0.0
                    Ajuste=0.0
                    NumeroMOQs=0
                    NumeroMOQCeil=0
                    NumeroMOQPurchase=0
                    MOQAjustadoFinal=0
                    NCajasAumentar=0
                    VolumenFinal=0.0
                    CajasPicking=0
                    #process vega
                    if product.MOQ.upper()=="CAJAS":
                        VolumenMOQ = product.Volumen
                    elif product.MOQ.upper()=="PALLET":
                        VolumenMOQ = product.Volumen*product.Amarre
                    elif product.MOQ.upper()=="CAMAS":
                        VolumenMOQ = product.Volumen*product.AmarreCama
                    Ajuste=((product.FinalPurchase*product.Volumen)/totalSumVol)*branch.DiferenciaVolumen
                    NumeroMOQs=Ajuste/VolumenMOQ
                    NumeroMOQCeil=round(NumeroMOQs)
                    NumeroMOQPurchase=product.FinalPurchase/(VolumenMOQ/product.Volumen)
                    MOQAjustadoFinal=NumeroMOQCeil+NumeroMOQPurchase
                    NCajasAumentar=(VolumenMOQ/product.Volumen)*MOQAjustadoFinal
                    VolumenFinal=NCajasAumentar*product.Volumen
                    CajasPicking=(NCajasAumentar/product.Amarre-math.floor(NCajasAumentar/product.Amarre))*product.Amarre
                    if branch.DiferenciaVolumen>0:
                        if (VolumenFinal+branchs['VolumenAumentado'][branch.Index])<=branch.DiferenciaVolumen:
                            #Llenado de Data
                            df.loc[product.Index, 'VolumenMOQ']=VolumenMOQ
                            df.loc[product.Index, 'Ajuste']=Ajuste
                            df.loc[product.Index, 'NumeroMOQs']=NumeroMOQs
                            df.loc[product.Index, 'NumeroMOQCeil']=NumeroMOQCeil
                            df.loc[product.Index, 'NumeroMOQPurchase']=NumeroMOQPurchase
                            df.loc[product.Index, 'MOQAjustadoFinal']=MOQAjustadoFinal
                            df.loc[product.Index, 'NCajasAumentar']=NCajasAumentar
                            df.loc[product.Index, 'VolumenFinal']=VolumenFinal
                            df.loc[product.Index, 'CajasPicking']=CajasPicking
                            branchs.loc[branch.Index, 'VolumenAumentado']+=VolumenFinal
                    else:
                        if (-VolumenFinal+branchs['VolumenAumentado'][branch.Index])>=branch.DiferenciaVolumen:
                            #Llenado de Data
                            df.loc[product.Index, 'VolumenMOQ']=VolumenMOQ
                            df.loc[product.Index, 'Ajuste']=Ajuste
                            df.loc[product.Index, 'NumeroMOQs']=NumeroMOQs
                            df.loc[product.Index, 'NumeroMOQCeil']=NumeroMOQCeil
                            df.loc[product.Index, 'NumeroMOQPurchase']=NumeroMOQPurchase
                            df.loc[product.Index, 'MOQAjustadoFinal']=MOQAjustadoFinal
                            df.loc[product.Index, 'NCajasAumentar']=-NCajasAumentar
                            df.loc[product.Index, 'VolumenFinal']=-VolumenFinal
                            df.loc[product.Index, 'CajasPicking']=-CajasPicking
                            branchs.loc[branch.Index, 'VolumenAumentado']+=-VolumenFinal
                    break
    def _compare_branch(self, branch, product):
        if type(branch.Sucursal)!=str or type(product.Sucursal)!=str or type(product.Categoria)!=str:
            return False
        partes=branch.Sucursal.split(' - ')
        if product.Categoria.upper()=='HOME CARE':
            if partes[0].upper()==product.Sucursal.upper() and partes[1].upper()==product.Categoria.upper():
                return True
        else:
            if partes[0].upper()==product.Sucursal.upper() and partes[1].upper()=="OTHERS":
                return True
        return False
    def _getTotalVolumenVega(self, df_group):
        # product._14 = "ABC XYZ"
        # product._12 = "Final Purchase"
        df_groupTemp=df_group.rename(columns = {'ABC XYZ':'ABCXYZ', 'Final Purchase':'FinalPurchase'})
        sumVol=0.0
        for prod in df_groupTemp.itertuples(index=True, name='PandasProductsTotal'):
            if prod.ABCXYZ=='CX' or prod.ABCXYZ=='CY' or prod.ABCXYZ=='CZ' or prod.Volumen==0 or prod.Comentario=='DESCONTINUADO': continue
            """if prod.MOQ.upper()=="CAJAS":
                sumVol+= prod.Volumen*prod.FinalPurchase
            elif prod.MOQ.upper()=="PALLET":
                sumVol+= prod.Volumen*prod.FinalPurchase*prod.Amarre
            elif prod.MOQ.upper()=="CAMAS":
                sumVol+= prod.Volumen*prod.FinalPurchase*prod.AmarreCama"""
            sumVol+= prod.Volumen*prod.FinalPurchase
        return sumVol
    def _fill_data_product_vega(self, df):
        dfTemp=df.rename(columns = {'Final Purchase':'FinalPurchase'})
        for product in dfTemp.itertuples(index=True, name='PandasProducts'):
            if product.Comentario=='DESCONTINUADO': continue
            df.loc[product.Index, 'CompraFinal']=product.FinalPurchase+product.NCajasAumentar
            if product.MOQ.upper()=="CAJAS":
                df.loc[product.Index, 'VolumenFinalTotal']=product.Volumen*(product.FinalPurchase+product.NCajasAumentar)
            elif product.MOQ.upper()=="PALLET":
                df.loc[product.Index, 'VolumenFinalTotal']=product.Volumen*(product.FinalPurchase+product.NCajasAumentar)*product.Amarre
            elif product.MOQ.upper()=="CAMAS":
                df.loc[product.Index, 'VolumenFinalTotal']=product.Volumen*(product.FinalPurchase+product.NCajasAumentar)*product.AmarreCama
    def _fill_data_branch_vega(self, df, branchs):
        dfTemp=df.rename(columns = {'PRECIO GIV':'PRECIOGIV', 'Venta mensual con factor':'VentaMensual', 'Inv + trans':'InvTrans', 'ABC XYZ':'ABCXYZ', 'Final Purchase':'FinalPurchase'})
        for branch in branchs.itertuples(index=True, name='PandasBranchs'):
            sumaPicking=0.0
            sumaFinalPurchase=0.0
            sumaInventario=0.0
            sumaVentaMensualFactor=0.0
            sumaInventarioAjuste=0.0
            compraFinal=0.0
            for product in dfTemp.itertuples(index=True, name='PandasProducts'):
                if self._compare_branch(branch, product):
                    sumaPicking+=product.CajasPicking
                    sumaFinalPurchase+=product.CompraFinal
                    sumaInventario+=product.Inventario
                    sumaInventarioAjuste+=(product.InvTrans + product.CompraFinal)
                    sumaVentaMensualFactor+=product.VentaMensual
                    compraFinal+=product.CompraFinal*product.PRECIOGIV
            branchs.loc[branch.Index, 'NCajasPicking']=sumaPicking/sumaFinalPurchase
            branchs.loc[branch.Index, 'DOHInicial']=sumaInventario*30/sumaVentaMensualFactor
            branchs.loc[branch.Index, 'DOHFinal']=sumaInventarioAjuste*30/sumaVentaMensualFactor
            branchs.loc[branch.Index, 'CompraFinal']=compraFinal
            branchs.loc[branch.Index, 'VolumenFinal']=branch.Volumen+branch.VolumenAumentado
    def _delete_unnecesary_fields(self):
        if not self.is_automatic() and self._inputs._numberOption==4:
            del self.__branchs['Cantidad']
            del self.__branchs['Ruta']
            """del self.__df_export_order['VolumenMOQ']
            del self.__df_export_order['Ajuste']
            del self.__df_export_order['NumeroMOQs']
            del self.__df_export_order['NumeroMOQCeil']
            del self.__df_export_order['MOQAjustadoFinal']"""
        else:
            #Eliminar para reporte Sucursal
            del self.__branchs['Cantidad']
            del self.__branchs['PickingMas']
            del self.__branchs['PickingMenos']
            #Eliminar para reporte General
            del self.__df_export_order['VolumenAjustado']
            del self.__df_export_order['NCajasAumentar']
            del self.__df_export_order['NCajasAumentarCeil']
            del self.__df_export_order['VolumenAumentarDisminuir']
            del self.__df_export_order['FinalPurchaseFinal']
            del self.__df_export_order['VolumenFinal']
            del self.__df_export_order['PorcentajePallet']
            del self.__df_export_order['AmarreCama']
    def get_data(self):
        return self.__df_export_order
    def get_branchs_data(self):
        return self.__branchs
    def get_trucks_consolidate(self):
        return self.__branchs_consolidation
    def is_automatic(self):
        return self._inputs.isAutomatic