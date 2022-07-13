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
            self.__df_export_order["Volumen"], self.__df_export_order["MOQ"], self.__df_export_order["Amarre"], self.__df_export_order["AmarreCama"], self.__df_export_order["VolumenMOQ"], self.__df_export_order["Ajuste"], self.__df_export_order["NumeroMOQs"], self.__df_export_order["NumeroMOQCeil"], self.__df_export_order["MOQAjustadoFinal"], self.__df_export_order["NCajasAumentar"]=[0.0, "", 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0]
            print('==>[INFO] Obteniendo Peso y volumen Total por Ean')
            self._volume_weight_x_ean(self.__df_export_order, None, inputs.df_weight_volume)
            print('==>[INFO] Obteniendo Peso y volumen Total por Sucursal')
            self.__branchs_other=copy.deepcopy(self.__branchs)
            self.__branchs['Sucursal']=self.__branchs['Sucursal'].apply(lambda x:x+" - Home Care")
            self.__branchs_other['Sucursal']=self.__branchs_other['Sucursal'].apply(lambda x:x+" - Others")
            self.__branchs=pd.concat([self.__branchs, self.__branchs_other], ignore_index = True)
            self._volume_weight_x_branch_vega(self.__df_export_order, self.__branchs)
            print('==>[INFO] Obteniendo Amarre por Ean')
            self._amarre_x_ean(self.__df_export_order, inputs.df_sku_data)
            if self.__errores is not None:
                report = Report(None)
                report.save_error(self.__errores)
                raise ValueError("[Error]======Revisar el Reporte de Errores ======[Error]")
            print('==>[INFO] Obteniendo MOQ por Ean')
            self._moq_x_ean(self.__df_export_order, inputs.df_moq)
            print('==>[INFO] Asignando camiones mas optimos')
            self._match_truck(self.__branchs, inputs.df_truck, True)
            print('==>[INFO] Separando Home Care y Otros')
            self.__home_care=self._get_home_care(self.__df_export_order)
            self.__others=self._get_others(self.__df_export_order)
            print('==>[INFO] procesando data Home Care')
            self._process_data_vega(self.__df_export_order, self.__home_care, self.__branchs)
            print('==>[INFO] procesando data Home Care')
            self._process_data_vega(self.__df_export_order, self.__others, self.__branchs)
        else:
            self.__df_export_order["Volumen"], self.__df_export_order["Amarre"], self.__df_export_order["VolumenAjustado"], self.__df_export_order["NCajasAumentar"], self.__df_export_order["NCajasAumentarCeil"], self.__df_export_order["VolumenAumentarDisminuir"], self.__df_export_order["FinalPurchaseFinal"], self.__df_export_order["VolumenFinal"], self.__df_export_order["PorcentajePallet"], self.__df_export_order["AjustePallet"], self.__df_export_order["VolumenFinalTotal"], self.__df_export_order["NCajasPicking"]=[0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
            print('==>[INFO] Obteniendo Peso y volumen Total por Ean y sucursal')
            self._volume_weight_x_ean(self.__df_export_order, self.__branchs, inputs.df_weight_volume)
            print('==>[INFO] Consolidando rutas')
            self.__branchs_consolidation=copy.deepcopy(self.__branchs).rename (columns = {'Sucursal':'Consolidado'})
            self.__branchs_consolidation=self._consolidation_routes(self.__branchs_consolidation, inputs.df_cmi)
            print('==>[INFO] Asignando camiones mas optimos')
            self.__branchs_consolidation=self._verify_match_truck(self.__branchs_consolidation, inputs.df_truck)
            print('==>[INFO] Asignando la diferencia de volumen faltante a cada sucursal')
            self._assign_weight_volume_difference_to_branch(self.__branchs, self.__branchs_consolidation)
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
            #self.__df_export_order = self._order_by_branch_coment_cod_price_volume_weight(self.__df_export_order)
            for i in range(len(self.__list_availables)):
                self.__list_availables[i]=self._order_by_cod_price_volume_weight(self.__list_availables[i])
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
    def _order_by_cod_price_volume_weight(self, df):
        return df.sort_values(by=['ABC XYZ', 'PRECIO GIV', 'Volumen'],ascending=[True, False, False])
    def _order_by_branch_coment_cod_price_volume_weight(self, df):
        return df.sort_values(by=['Sucursal', 'Comentario', 'ABC XYZ', 'PRECIO GIV', 'Volumen'],ascending=[True, True, True, False, False])
    def _volume_weight_x_ean(self, eans, branchs, volumes):
        sinVolumenPeso=[]
        volumenCero=[]
        for ean in eans.itertuples(index=True, name='PandasEANS'):
            encontrado=False
            if ean.Comentario=='DESCONTINUADO': continue
            for vol in volumes.itertuples(index=True, name='PandasVolPes'):
                if ean.EAN==vol.EAN:
                    encontrado=True
                    if vol.Volumen==0.0:
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
            ErroresEANSVolu.loc[:,'Comentario'] = 'Volumen de EAN "no Descontinuado" no puede ser igual a cero'
            self.__errores= pd.concat([self.__errores, ErroresEANSVolu], ignore_index = True)
        if branchs is not None:
            self._volume_weight_x_branch(eans, branchs)
    def _volume_weight_x_branch(self, eans, branchs):
        # ean._9 = "Final Purchase"
        for ean in eans.itertuples(index=True, name='PandasEANS'):
            if ean.Comentario=='DESCONTINUADO': continue
            for bran in branchs.itertuples(index=True, name='PandasSucursales'):
                if ean.Sucursal==bran.Sucursal:
                    branchs.loc[bran.Index, 'Volumen'] += ean.Volumen * ean._9
                    break
    def _amarre_x_ean(self, df, skus):
        sinAmarre=[]
        for ean in df.itertuples(index=True, name='PandasEANS'):
            encontrado=False
            if ean.Comentario=='DESCONTINUADO': continue
            for sk in skus.itertuples(index=True, name='PandasSKUS'):
                if ean.EAN==sk.EAN:
                    encontrado=True
                    df.loc[ean.Index, 'Amarre'] = sk.AMARRE
                    df.loc[ean.Index, 'AmarreCama']=sk._6
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
            can_consolidate.loc[co.Index, 'Sucursal']=co.Sucursal.replace("Dijisa ", '').replace("AGA ", '').replace("Moran ", '').replace("Digumisac ", '').replace("DEL PRADO - ", '').upper()
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
            for co in can_consolidate.itertuples(index=True, name='PandasBranchs'):
                if gr.Ruta==co.Ruta:
                    names_co.append(''+co.Sucursal)
                    amount_co +=branchs['Cantidad'][branchs[branchs['Consolidado'] == co.Sucursal].index.values[0]]
                    volume_co +=branchs['Volumen'][branchs[branchs['Consolidado'] == co.Sucursal].index.values[0]]
                    tipo_co=co.TipoCamion
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
                if name==name_branch:
                    return True
        return False
    def _verify_match_truck(self, consolidation, trucks):
        trucks_order=(self._order_by(trucks,'Volumen Gramel', False)).rename (columns = {'Volumen Gramel':'Volumen', 'LTL?':'LTL'})
        for bran in consolidation.itertuples(index=True, name='PandasBranchs'):
            volumeDifference=float('inf')
            for truck in trucks_order.itertuples(index=True, name='PandasTruck'):
                if math.fabs(truck.Volumen-bran.Volumen)<math.fabs(volumeDifference) and self._verify_LTL(bran.TipoCamion, truck):
                    list_trucks=[]
                    list_trucks.append(truck.TIPO)
                    consolidation.loc[bran.Index, 'Camion'] = str(list_trucks)
                    consolidation.loc[bran.Index, 'DiferenciaVolumen']=truck.Volumen-bran.Volumen
                    volumeDifference=truck.Volumen-bran.Volumen
                for truck2 in trucks_order.itertuples(index=True, name='PandasTruck2'):
                    if math.fabs(truck.Volumen+truck2.Volumen-bran.Volumen)<math.fabs(volumeDifference) and self._verify_LTL(bran.TipoCamion, truck) and self._verify_LTL(bran.TipoCamion, truck2):
                        list_trucks=[]
                        list_trucks.append(truck.TIPO)
                        list_trucks.append(truck2.TIPO)
                        consolidation.loc[bran.Index, 'Camion'] = str(list_trucks)
                        consolidation.loc[bran.Index, 'DiferenciaVolumen']=truck.Volumen+truck2.Volumen-bran.Volumen
                        volumeDifference=truck.Volumen+truck2.Volumen-bran.Volumen
                    for truck3 in trucks_order.itertuples(index=True, name='PandasTruck2'):
                        if math.fabs(truck.Volumen+truck2.Volumen+truck3.Volumen-bran.Volumen)<math.fabs(volumeDifference) and self._verify_LTL(bran.TipoCamion, truck) and self._verify_LTL(bran.TipoCamion, truck2) and self._verify_LTL(bran.TipoCamion, truck3):
                            list_trucks=[]
                            list_trucks.append(truck.TIPO)
                            list_trucks.append(truck2.TIPO)
                            list_trucks.append(truck3.TIPO)
                            consolidation.loc[bran.Index, 'Camion'] = str(list_trucks)
                            consolidation.loc[bran.Index, 'DiferenciaVolumen']=truck.Volumen+truck2.Volumen+truck3.Volumen-bran.Volumen
                            volumeDifference=truck.Volumen+truck2.Volumen+truck3.Volumen-bran.Volumen
        return consolidation
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
    def _assign_weight_volume_difference_to_branch(self, branchs, branchs_consolidation):
        for consolidation in branchs_consolidation.itertuples(index=True, name='PandasConsolidation'):
            list_names=consolidation.Consolidado
            for name in list_names:
                for branch in branchs.itertuples(index=True, name='PandasBranch'):
                    if branch.Sucursal==name:
                        branchs.loc[branch.Index, 'DiferenciaVolumen']=branch.Volumen/consolidation.Volumen*consolidation.DiferenciaVolumen
    def _match_data_client(self, branchs, data_client):
        # client._7 = "DOH Target"
        # client._8 = "DOH +"
        # client._9 = "DOH Min"
        # client._11 = "% Picking +"
        # client._12 = "% Picking -"
        for client in data_client.itertuples(index=True, name='PandasClient'):
            data_client.loc[client.Index, 'Sucursal']=client.Sucursal.replace("Dijisa ", '').replace("AGA ", '').replace("Moran ", '').replace("Digumisac ", '').replace("DEL PRADO - ", '').upper()
        for client in data_client.itertuples(index=True, name='PandasClient'):
            for bran in branchs.itertuples(index=True, name='PandasBranchs'):
                if bran.Sucursal==client.Sucursal:
                    #branchs.loc[bran.Index, 'DOH']=client._7
                    #branchs.loc[bran.Index, 'DOHMas']=client._8
                    #branchs.loc[bran.Index, 'DOHMin']=client._9
                    branchs.loc[bran.Index, 'PickingMas']=client._11
                    branchs.loc[bran.Index, 'PickingMenos']=client._12
    def _group_available(self, df):
        list_availables=[]
        df_branchs=df.groupby('Sucursal')
        for branch_name in df_branchs.groups.keys():
            branch=df_branchs.get_group(branch_name)
            list_availables.append(branch.groupby('Comentario').get_group('DISPONIBLE'))
        return list_availables
    def _assigning_volume_weigth_remaining(self, df, group_df, branchs):
        # ean._9 = "Final Purchase"
        # product._16 = "ABC XYZ"
        totalSumVol=self._getTotalVolumen(group_df)
        for product in group_df.itertuples(index=True, name='PandasProducts'):
            if product._16=='CX' or product._16=='CY' or product._16=='CZ' or product.Volumen==0 or product.Comentario=='DESCONTINUADO': continue
            for branch in branchs.itertuples(index=True, name='PandasBranchs'):
                if branch.DiferenciaVolumen==0.0: continue
                elif (branch.DiferenciaVolumen - branch.VolumenAumentado)==0.0: continue
                elif product.Sucursal==branch.Sucursal:
                    VolumenAjustado=((product._9*product.Volumen)/totalSumVol)*branch.DiferenciaVolumen
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
                        FinalPurchaseFinal= NCajasAumentarCeil+product._9
                        VolumenFinal= product.Volumen*product._9+VolumenAumentarDisminuir
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
                            if VolumenFinalTotal<product.Volumen*product._9:
                                continue
                            if (VolumenFinalTotal-product.Volumen*product._9+branchs['VolumenAumentado'][branch.Index])<=branch.DiferenciaVolumen:
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
                                branchs.loc[branch.Index, 'VolumenAumentado']+=VolumenFinalTotal-product.Volumen*product._9-lastVolume
                                lastVolume=VolumenFinalTotal-product.Volumen*product._9
                        else:
                            AjustePallet = PorcentajePallet*product.Amarre
                            VolumenFinalTotal=AjustePallet*product.Volumen
                            if VolumenFinalTotal>product.Volumen*product._9:
                                continue
                            if (VolumenFinalTotal-product.Volumen*product._9+branchs['VolumenAumentado'][branch.Index])>=branch.DiferenciaVolumen:
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
                                branchs.loc[branch.Index, 'VolumenAumentado']+=VolumenFinalTotal-product.Volumen*product._9-lastVolume
                                lastVolume=VolumenFinalTotal-product.Volumen*product._9
                    break
    def _getTotalVolumen(self, df_group):
        sumVol=0.0
        for product in df_group.itertuples(index=True, name='PandasProducts'):
            if product._16=='CX' or product._16=='CY' or product._16=='CZ' or product.Volumen==0 or product.Comentario=='DESCONTINUADO': continue
            sumVol+=product.Volumen*product._9
        return sumVol
    def _fill_route_truck_branch(self, branchs, branchs_consolidation):
        for consolidation in branchs_consolidation.itertuples(index=True, name='PandasConsolidation'):
            list_names=consolidation.Consolidado
            for name in list_names:
                for branch in branchs.itertuples(index=True, name='PandasBranch'):
                    if branch.Sucursal==name:
                        branchs.loc[branch.Index, 'Ruta']=consolidation.Ruta
                        branchs.loc[branch.Index, 'Camion']=consolidation.Camion
    def _fill_data_product(self, df):
        for product in df.itertuples(index=True, name='PandasProducts'):
            if product.Comentario=='DESCONTINUADO': continue
            if product.NCajasAumentarCeil==0.0:
                df.loc[product.Index, 'AjustePallet']=product._9
                df.loc[product.Index, 'VolumenFinalTotal']=product.Volumen*product._9
            ppallet=df['AjustePallet'][product.Index]/product.Amarre
            if ppallet<1.0:
                df.loc[product.Index, 'NCajasPicking']=ppallet*product.Amarre
            else:
                df.loc[product.Index, 'NCajasPicking']=(ppallet-math.floor(ppallet))*product.Amarre
    def _fill_data_branch(self, df, branchs):
        # product._11 = "PRECIO GIV"
        # product._12 = "Venta Mensual con factor"
        # product._22 = "Inv + Trans"
        for branch in branchs.itertuples(index=True, name='PandasBranchs'):
            sumaPicking=0.0
            sumaFinalPurchase=0.0
            sumaInventario=0.0
            sumaVentaMensualFactor=0.0
            sumaInventarioAjuste=0.0
            compraFinal=0.0
            for product in df.itertuples(index=True, name='PandasProducts'):
                if product.Sucursal==branch.Sucursal:
                    sumaPicking+=product.NCajasPicking
                    sumaFinalPurchase+=product.AjustePallet
                    sumaInventario+=product.Inventario
                    sumaInventarioAjuste+=(product._22 + product.AjustePallet)
                    sumaVentaMensualFactor+=product._12
                    compraFinal+=product.AjustePallet*product._11
            branchs.loc[branch.Index, 'NCajasPicking']=sumaPicking/sumaFinalPurchase
            branchs.loc[branch.Index, 'DOHInicial']=sumaInventario*30/sumaVentaMensualFactor
            branchs.loc[branch.Index, 'DOHFinal']=sumaInventarioAjuste*30/sumaVentaMensualFactor
            branchs.loc[branch.Index, 'CompraFinal']=compraFinal
            branchs.loc[branch.Index, 'VolumenFinal']=branch.Volumen+branch.VolumenAumentado
    #-------------Grupo Vega----------------
    def _volume_weight_x_branch_vega(self, eans, branchs):
        # ean._12 = "Final Purchase"
        eans.fillna('N/A')
        for ean in eans.itertuples(index=True, name='PandasEANS'):
            if ean.Comentario=='DESCONTINUADO': continue
            for bran in branchs.itertuples(index=True, name='PandasSucursales'):
                if self._compare_sucursal_vega(bran.Sucursal, ean):
                    branchs.loc[bran.Index, 'Volumen'] += ean.Volumen * ean._12
                    break
    def _compare_sucursal_vega(self, sucursal_name, ean):
        if type(ean.Categoria)==float:
            return False
        elif ean.Categoria.upper()=='HOME CARE':
            if sucursal_name.upper()==ean.Sucursal.upper()+' - HOME CARE':
                return True
        else:
            if sucursal_name.upper()==ean.Sucursal.upper()+' - OTHERS':
                return True
        return False
    def _get_home_care(self, df):
        return df.groupby('Categoria').get_group('HOME CARE')
    def _get_others(self, df):
        others=None
        df_categories=df.groupby('Categoria')
        for categorie_name in df_categories.groups.keys():
            if categorie_name!='HOME CARE':
                if others is None:
                    others=df.groupby('Categoria').get_group(categorie_name)
                else:
                    others = pd.concat([others, df.groupby('Categoria').get_group(categorie_name)], ignore_index = True)
        return others
    def _moq_x_ean(self, eans, moqs):
        # ean._2 = "Codigo EAN"
        sinMOQ=[]
        for ean in eans.itertuples(index=True, name='PandasEANS'):
            encontrado=False
            if ean.Comentario=='DESCONTINUADO': continue
            for moq in moqs.itertuples(index=True, name='PandasMOQ'):
                if ean.EAN==moq._2:
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
    def _match_truck(self, consolidation, trucks, isGranel):
        trucks_order=(self._order_by(trucks,'Volumen Gramel', False)).rename (columns = {'Volumen Gramel':'Volumen', 'LTL?':'LTL', 'Min Pallet':'MinPallet', 'Max Pallet':'MaxPallet'})
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
            if math.fabs(truck.Volumen+accumulateVolume-branch.Volumen)<math.fabs(volumeDifferenceTempChange) and self._verify_if_belong_branch(branch.Sucursal, truck):
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
    def _verify_if_belong_branch(self, sucursal_name, truck):
        if type(truck.Sucursal)!=str:
            return False
        elif truck.Sucursal.upper()=="TODOS" or sucursal_name.upper()==(truck.Sucursal.upper()+" - OTHERS") or sucursal_name.upper()==(truck.Sucursal.upper()+" - HOME CARE"):
            return True
        return False
    def _process_data_vega(self, df, group_df, branchs):
        # product._12 = "Final Purchase"
        # product._14 = "ABC XYZ"
        totalSumVol=self._getTotalVolumenVega(group_df)
        if totalSumVol==0.0: return
        for product in group_df.itertuples(index=True, name='PandasProducts'):
            #if product._16=='CX' or product._16=='CY' or product._16=='CZ' or product.Volumen==0 or product.Comentario=='DESCONTINUADO': continue
            if product._14=='CX' or product._14=='CY' or product._14=='CZ' or product.Volumen==0 or product.Comentario=='DESCONTINUADO': continue
            for branch in branchs.itertuples(index=True, name='PandasBranchs'):
                if branch.DiferenciaVolumen==0.0: continue
                elif (branch.DiferenciaVolumen - branch.VolumenAumentado)==0.0: continue
                elif self._compare_branch(branch, product):
                    VolumenMOQ=0.0
                    Ajuste=((product._12*product.Volumen)/totalSumVol)*branch.DiferenciaVolumen
                    NumeroMOQs=0
                    NumeroMOQCeil=0.0
                    MOQAjustadoFinal=0.0
                    NCajasAumentar=0
                    if product.MOQ=="PALLET":
                        VolumenMOQ = product.Volumen*product.Amarre
                    elif product.MOQ=="CAMAS":
                        VolumenMOQ = product.Volumen*product.AmarreCama
                    else:
                        continue
                    NumeroMOQs=Ajuste*VolumenMOQ
                    try:
                        NumeroMOQCeil=round(VolumenMOQ)
                    except Exception as e:
                        continue
                    MOQAjustadoFinal=NumeroMOQCeil*VolumenMOQ
                    NCajasAumentar=MOQAjustadoFinal/product.Volumen
                    if MOQAjustadoFinal<product.Volumen*product._12:
                        continue
                    if (MOQAjustadoFinal-product.Volumen*product._12+branchs['VolumenAumentado'][branch.Index])<=branch.DiferenciaVolumen:
                        #Llenado de Data
                        df.loc[product.Index, 'VolumenMOQ']=VolumenMOQ
                        df.loc[product.Index, 'Ajuste']=Ajuste
                        df.loc[product.Index, 'NumeroMOQs']=NumeroMOQs
                        df.loc[product.Index, 'NumeroMOQCeil']=NumeroMOQCeil
                        df.loc[product.Index, 'MOQAjustadoFinal']=MOQAjustadoFinal
                        df.loc[product.Index, 'NCajasAumentar']=NCajasAumentar
                        branchs.loc[branch.Index, 'VolumenAumentado']+=MOQAjustadoFinal-product.Volumen*product._12
    def _compare_branch(self, branch, product):
        if type(branch.Sucursal)!=str or type(product.Sucursal)!=str or type(product.Categoria)!=str:
            return False
        partes=branch.Sucursal.split(' ')
        for i in range(len(partes)):
            if product.Categoria.upper()=='HOME CARE':
                if partes[i].upper()==product.Sucursal.upper() and branch.Sucursal.upper().find(product.Categoria.upper())!=-1:
                    return True
            else:
                if partes[i].upper()==product.Sucursal.upper():
                    return True
        return False
    def _getTotalVolumenVega(self, df_group):
        sumVol=0.0
        for prod in df_group.itertuples(index=True, name='PandasProductsTotal'):
            #if product._16=='CX' or product._16=='CY' or product._16=='CZ' or product.Volumen==0 or product.Comentario=='DESCONTINUADO': continue
            if prod._14=='CX' or prod._14=='CY' or prod._14=='CZ' or prod.Volumen==0 or prod.Comentario=='DESCONTINUADO': continue
            if prod.MOQ.upper()=="PALLET":
                sumVol+= prod.Volumen*prod.Amarre
            elif prod.MOQ.upper()=="CAMAS":
                sumVol+= prod.Volumen*prod.AmarreCama
        return sumVol
    def _delete_unnecesary_fields(self):
        if not self.is_automatic() and self._inputs._numberOption==4:
            pass
        else:
            del self.__branchs['Cantidad']
            #del self.__branchs['DOH']
            #del self.__branchs['DOHMas']
            #del self.__branchs['DOHMin']
            del self.__branchs['PickingMas']
            del self.__branchs['PickingMenos']
            del self.__branchs['DiferenciaVolumen']
            #del self.__df_export_order['Volumen']
            #Eliminar para reporte
            """del self.__df_export_order['VolumenAjustado']
            del self.__df_export_order['NCajasAumentar']
            del self.__df_export_order['NCajasAumentarCeil']
            del self.__df_export_order['VolumenAumentarDisminuir']
            del self.__df_export_order['FinalPurchaseFinal']
            del self.__df_export_order['VolumenFinal']
            del self.__df_export_order['PorcentajePallet']"""
    def get_data(self):
        return self.__df_export_order
    def get_branchs_data(self):
        return self.__branchs
    def get_trucks_consolidate(self):
        return self.__branchs_consolidation
    def is_automatic(self):
        return self._inputs.isAutomatic