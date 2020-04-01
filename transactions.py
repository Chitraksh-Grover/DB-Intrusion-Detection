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
        
    createProfile(profile,'brokervolume')
    
    del industry_id,company_id,security_symbol,trade,qty,price,sectors
    return sector_name , brokers , volume

############################CUSTOMER POSITION TRANSACTION######################
def customerposition(cust_id='', get_history='', tax_id=''):
    if cust_id == '' or  get_history== '' or tax_id== '':
        print('parameter mising , choosing random parameters ###############')
        cust_id = np.random.randint(1)
        get_history = np.random.randint(1)
        if cust_id == 0:
            customers = list(dg.Customer['C_TAX_ID'])
            tax_id = customers[np.random.randint(len(customers))]
            cust_id = dg.Customer.loc[dg.Customer['C_TAX_ID']==tax_id,'C_ID'].values[0]
        else:
            customers = list(dg.Customer['C_ID'])
            cust_id = customers[np.random.randint(len(customers))]
    
    customer_info = dg.Customers.loc[dg.Custmers['TAX_ID']==cust_id,[ 'C_ST_ID',
    'C_L_NAME','C_F_NAME','C_M_NAME','C_GNDR','C_TIER','C_DOB','C_AD_ID',
    'C_CTRY_1','C_AREA_1','C_LOCAL_1','C_EXT_1','C_CTRY_2','C_AREA_2','C_LOCAL_2',
    'C_EXT_2','C_CTRY_3','C_AREA_3','C_LOCAL_3','C_EXT_3','C_EMAIL_1','C_EMAIL_2']]
    
    customeraccount = dg.CustomerAccount.loc[dg.CustomerAccount['CA_C_ID']==cust_id,['CA_ID','CA_BAL']]
    account_id = list(customeraccount['CA_ID'])
    cash_balance = list(customeraccount['CA_BAL'])
    #max account length is 10
    if len(account_id)>10:
        del account_id[10:]
        del cash_balance[10:]
    
    assets_total = []
    for account in account_id:
        holdingsummary = dg.HoldingHistory.loc[dg.HoldingSummary['HS_CA_ID'] == account,['HS_QTY','HS_S_SYMB']]
        lasttrade = dg.LastTrade.loc[dg.LastTrade['LT_S_SYMB'].isin(list(holdingsummary['HS_S_SYMB'])),['LT_S_SYMB','LT_PRICE']]
        symbol = list(holdingsummary['HS_S_SYMB'])
        qty = list(holdingsummary['HS_QTY'])
        asset = 0
        for i in range(len(symbol)):
            price = lasttrade.loc[lasttrade['LT_S_SYMB']==symbol[i],'LT_PRICE'].values[0]
            try:
                asset = asset + price * qty[i]
            except:
                continue
        assets_total.append(asset)
        
        return customer_info,cash_balance,assets_total,
            
    
        
        
    
    