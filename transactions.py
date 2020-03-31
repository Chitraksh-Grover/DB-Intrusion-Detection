#creating different transaction on the database created in flat_out folder
#transactions are defined in the TPC-E specification pdf. 

import datagen as dg
import numpy as np
import statistics

##########################profile creation#####################################
def createProfile(profile):
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
    urv_file.write('\n')
    urv_file.close()
    
    spm_file = open('spm_file','a')
    spm_file.write(str(access_sequence))
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
        
    createProfile(profile)
    
    del industry_id,company_id,security_symbol,trade,qty,price,sectors
    return sector_name , brokers , volume

    