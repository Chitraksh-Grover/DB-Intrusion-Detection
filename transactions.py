#creating different transaction on the database created in flat_out folder
#transactions are defined in the TPC-E specification pdf. 

import datagen as dg
import numpy as np

#####################BROKER VOLUME TRANSACTION################################# 
#broker manager 
def brokervolume():
    min_broker_len = 5
    max_broker_len = 10
    broker_len = np.random.randint(min_broker_len,max_broker_len+1)
    brokers = list(dg.Broker['B_ID'])
    for i in range (max_broker_len - broker_len):
        del brokers[np.random.randint(len(brokers))]
        
        sectors = list(dg.Sector['SC_ID'])
        sector_id = sectors[np.random.randint(len(sectors))]
        sector_name = dg.Sector.loc[dg.Sector['SC_ID'] == sector_id,'SC_NAME'].values[0]
        
        volume = []
        
        for broker in brokers:
            industry_id = list(dg.Industry.loc[dg.Industry['IN_SC_ID'] == sector_id,'IN_ID'])
            company_id = list(dg.Company.loc[dg.Company['CO_IN_ID'].isin(industry_id),'CO_ID'])
            security_symbol = list(dg.Security.loc[dg.Security['S_CO_ID'].isin(company_id),'S_SYMB'])
            trade = dg.TradeRequest.loc[(dg.TradeRequest['TR_B_ID'] == broker) & (dg.TradeRequest['TR_S_SYMB'].isin(security_symbol) ),'TR_QTY']
            qty = trade['TR_QTY']
            price = trade['TR_BID_PRICE']
            v = qty*price
            v = v.sum()
            volume.append(v)
        
        del industry_id,company_id,security_symbol,trade,qty,price,sectors
        return sector_name , brokers , volume

    