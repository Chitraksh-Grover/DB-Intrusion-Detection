#creating different transaction on the database created in flat_out folder
#transactions are defined in the TPC-E specification pdf. 

import datagen as dg
import numpy as np
import statistics

##########################profile creation#####################################
def createProfile(profile,trans):
    URV = ['o' for i in range(dg.URV_size)]
    access_sequence = list(profile.keys())
    for feature in access_sequence:
        if dg.URV_feature_type[feature] == 'C':
            URV[dg.URV_feature_index[feature]] = len(set(profile[feature]))
            URV[dg.URV_feature_index[feature] + 1] = len(profile[feature])
        else:
            try:
                URV[dg.URV_feature_index[feature]] = statistics.mean(profile[feature])
                URV[dg.URV_feature_index[feature]+1] = statistics.median(profile[feature])
                URV[dg.URV_feature_index[feature]+2] = statistics.stdev(profile[feature])
                URV[dg.URV_feature_index[feature]+3] = max(profile[feature])
                URV[dg.URV_feature_index[feature]+4] = min(profile[feature])
            except:
                continue
    urv_file = open('urv_file','a')
    urv_file.write(str(URV))
    urv_file.write(trans)
    urv_file.write('\n')
    urv_file.close()
    
    spm_file = open('spm_file','a')
    spm_file.write(str(access_sequence))
    spm_file.write(trans)
    spm_file.write('\n')
    spm_file.close()

#####################BROKER VOLUME TRANSACTION################################# 
#broker manager 
def brokervolume():
    profile = {'B_ID':[],'SC_ID':[],'SC_NAME':[],'IN_SC_ID':[],'IN_ID':[],
               'CO_IN_ID':[],'CO_ID':[],'S_CO_ID':[],'S_SYMB':[],'TR_B_ID':[],
               'TR_S_SYMB':[],'TR_QTY':[],'TR_BID_PRICE':[]}
    min_broker_len = 5
    max_broker_len = 10
    broker_len = np.random.randint(min_broker_len,max_broker_len+1)
    brokers = list(dg.Broker['B_ID'])
    for i in range (max_broker_len - broker_len):
        del brokers[np.random.randint(len(brokers))]
    
    profile['B_ID'].extend(brokers)
    
    sectors = list(dg.Sector['SC_ID'])
    sector_id = sectors[np.random.randint(len(sectors))]
    sector_name = dg.Sector.loc[dg.Sector['SC_ID'] == sector_id,'SC_NAME'].values[0]
    
    profile['SC_ID'].extend(list(sector_id))
    profile['SC_NAME'].append(sector_name)
        
    volume = []
        
    industry_id = list(dg.Industry.loc[dg.Industry['IN_SC_ID'] == sector_id,'IN_ID'])
    profile['IN_SC_ID'].extend(list(sector_id))
    profile['IN_ID'].extend(industry_id)
    company = dg.Company.loc[dg.Company['CO_IN_ID'].isin(industry_id),['CO_IN_ID','CO_ID']]
    company_id = list(company['CO_ID'])
    profile['CO_IN_ID'].extend(list(company['CO_IN_ID']))
    profile['CO_ID'].extend(company_id)
    security = dg.Security.loc[dg.Security['S_CO_ID'].isin(company_id),['S_CO_ID','S_SYMB']]
    security_symbol = list(security['S_SYMB'])
    profile['S_CO_ID'].extend(list(security['S_CO_ID']))
    profile['S_SYMB'].extend(security_symbol)
    
    for broker in brokers:
       trade = dg.TradeRequest.loc[(dg.TradeRequest['TR_B_ID'] == broker) & (dg.TradeRequest['TR_S_SYMB'].isin(security_symbol) ),['TR_S_SYMB','TR_QTY','TR_BID_PRICE']]
       qty = trade['TR_QTY']
       price = trade['TR_BID_PRICE']
       profile['TR_B_ID'].append(broker)
       profile['TR_S_SYMB'].extend(list(trade['TR_S_SYMB']))
       profile['TR_QTY'].extend(list(qty))
       profile['TR_BID_PRICE'].extend(list(price))
       v = qty*price
       v = v.sum()
       volume.append(v)
        
    createProfile(profile,' brokervolume')
    
    del industry_id,company_id,security_symbol,trade,qty,price,sectors
    return sector_name , brokers , volume

############################CUSTOMER POSITION TRANSACTION######################
def customerposition(cust_id='', get_history='', tax_id='',account_id_idx=''):
    profile = {}
    if cust_id == ''  or  get_history== '' or tax_id== '':
        print('parameter mising , choosing random parameters ###############')
        cust_id = np.random.randint(2)
        get_history = np.random.randint(2)
        if cust_id == 0:
            customers = list(dg.Customer['C_TAX_ID'])
            tax_id = customers[np.random.randint(len(customers))]
            profile['C_TAX_ID'] = [tax_id]
            cust_id = dg.Customer.loc[dg.Customer['C_TAX_ID']==tax_id,'C_ID'].values[0]
            profile['C_ID'] = [cust_id]
        else:
            customers = list(dg.Customer['C_ID'])
            cust_id = customers[np.random.randint(len(customers))]
            profile['C_ID'] = [cust_id]
            
    ###############################FRAME 2###################################        
    if get_history == 1:
        if account_id_idx == '':
            account_id = list(dg.CustomerAccount.loc[dg.CustomerAccount['CA_C_ID']==cust_id,'CA_ID'])
            account_id_idx = account_id[np.random.randint(len(account_id))]
            profile['CA_C_ID'] = [cust_id]
            profile['CA_ID']  = [account_id_idx]
        
        trade = dg.Trade.loc[dg.Trade['T_CA_ID']==account_id_idx,['T_ID','T_CA_ID','T_S_SYMB','T_QTY','T_DTS']]
        trade = trade[::-1]
        trade = trade.iloc[:10]
        trade = trade[::-1]
        profile['T_ID'] = list(trade['T_ID'])
        profile['T_CA_ID'] = list(trade['T_CA_ID'])
        profile['T_S_SYMB'] = list(trade['T_S_SYMB'])
        profile['T_QTY'] = list(trade['T_QTY'])
        profile['T_DTS'] = list(trade['T_DTS'])
        tradehistory = dg.TradeHistory.loc[dg.TradeHistory['TH_T_ID'].isin(list(trade['T_ID'])),['TH_T_ID','TH_ST_ID','TH_DTS']]
        tradehistory = tradehistory[::-1]
        tradehistory = tradehistory[:30]
        profile['TH_T_ID'] = list(tradehistory['TH_T_ID'])
        profile['TH_ST_ID'] = list(tradehistory['TH_ST_ID'])
        profile['TH_DTS'] = list(tradehistory['TH_DTS'])
        status = dg.StatusType.loc[dg.StatusType['ST_ID'].isin(list(tradehistory['TH_ST_ID'])),['ST_ID','ST_NAME']]
        result =trade.set_index('T_ID',drop=False).join(tradehistory.set_index('TH_T_ID',drop=False))
        result =result.set_index('TH_ST_ID',drop=False).join(status.set_index('ST_ID',drop=False))
        profile['ST_ID'] = list(result['ST_ID'])
        profile['ST_NAME'] = list(result['ST_NAME'])
        createProfile(profile,' customerposition '+str(cust_id))
        return cust_id,tax_id,account_id_idx,result[['T_ID','T_S_SYMB','T_QTY','ST_NAME','TH_DTS']]
    #########################################################################        
    
    customer_info = dg.Customer.loc[dg.Customer['C_ID']==cust_id,[ 'C_ST_ID',
    'C_L_NAME','C_F_NAME','C_M_NAME','C_GNDR','C_TIER','C_DOB','C_AD_ID',
    'C_CTRY_1','C_AREA_1','C_LOCAL_1','C_EXT_1','C_CTRY_2','C_AREA_2','C_LOCAL_2',
    'C_EXT_2','C_CTRY_3','C_AREA_3','C_LOCAL_3','C_EXT_3','C_EMAIL_1','C_EMAIL_2']]
    profile['C_ST_ID'] = [customer_info['C_ST_ID'].values[0]]
    profile['C_L_NAME'] = [customer_info['C_L_NAME'].values[0]]
    profile['C_F_NAME'] = [customer_info['C_F_NAME'].values[0]]
    profile['C_M_NAME'] = [customer_info['C_M_NAME'].values[0]]
    profile['C_GNDR'] = [customer_info['C_GNDR'].values[0]]
    profile['C_TIER'] = [customer_info['C_TIER'].values[0]]
    profile['C_DOB'] = [customer_info['C_DOB'].values[0]]
    profile['C_AD_ID'] = [customer_info['C_AD_ID'].values[0]]
    profile['C_CTRY_1'] = [customer_info['C_CTRY_1'].values[0]]
    profile['C_AREA_1'] = [customer_info['C_AREA_1'].values[0]]
    profile['C_LOCAL_1'] = [customer_info['C_LOCAL_1'].values[0]]
    profile['C_EXT_1'] = [customer_info['C_EXT_1'].values[0]]
    profile['C_CTRY_2'] = [customer_info['C_CTRY_2'].values[0]]
    profile['C_AREA_2'] = [customer_info['C_AREA_2'].values[0]]
    profile['C_LOCAL_2'] = [customer_info['C_LOCAL_2'].values[0]]
    profile['C_EXT_2'] = [customer_info['C_EXT_2'].values[0]]
    profile['C_CTRY_3'] = [customer_info['C_CTRY_3'].values[0]]
    profile['C_AREA_3'] = [customer_info['C_AREA_3'].values[0]]
    profile['C_LOCAL_3'] = [customer_info['C_LOCAL_3'].values[0]]
    profile['C_EXT_3'] = [customer_info['C_EXT_3'].values[0]]
    profile['C_EMAIL_1'] = [customer_info['C_EMAIL_1'].values[0]]
    profile['C_EMAIL_2'] = [customer_info['C_EMAIL_2'].values[0]]
    
    customeraccount = dg.CustomerAccount.loc[dg.CustomerAccount['CA_C_ID']==cust_id,['CA_C_ID','CA_ID','CA_BAL']]
    account_id = list(customeraccount['CA_ID'])
    cash_balance = list(customeraccount['CA_BAL'])
    #max account length is 10
    if len(account_id)>10:
        del account_id[10:]
        del cash_balance[10:]
        
    profile['CA_C_ID'] = [cust_id for i in range(len(account_id))]
    profile['CA_ID'] = account_id
    profile['CA_BAL'] = cash_balance
    
    profile['HS_CA_ID'] = []
    profile['HS_S_SYMB'] = []
    profile['HS_QTY'] = []
    profile['LT_S_SYMB'] = []
    profile['LT_PRICE'] = []
    
    assets_total = []
    for account in account_id:
        holdingsummary = dg.HoldingSummary.loc[dg.HoldingSummary['HS_CA_ID'] == account,['HS_CA_ID','HS_QTY','HS_S_SYMB']]
        lasttrade = dg.LastTrade.loc[dg.LastTrade['LT_S_SYMB'].isin(list(holdingsummary['HS_S_SYMB'])),['LT_S_SYMB','LT_PRICE']]
        symbol = list(holdingsummary['HS_S_SYMB'])
        qty = list(holdingsummary['HS_QTY'])
        profile['HS_CA_ID'].extend(list(holdingsummary['HS_CA_ID'])) 
        profile['HS_S_SYMB'].extend(symbol)
        profile['HS_QTY'].extend(qty)
        profile['LT_S_SYMB'].extend(list(lasttrade['LT_S_SYMB']))
        profile['LT_PRICE'].extend(list(lasttrade['LT_PRICE']))
        asset = 0
        for i in range(len(symbol)):
            price = lasttrade.loc[lasttrade['LT_S_SYMB']==symbol[i],'LT_PRICE'].values[0]
            try:
                asset = asset + price * qty[i]
            except:
                continue
        assets_total.append(asset)
        
        createProfile(profile," customerposition "+str(cust_id))
        return cust_id,tax_id,account_id,customer_info,cash_balance,assets_total
            
    
        
        
    
    