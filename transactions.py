#creating different transaction on the database created in flat_out folder
#transactions are defined in the TPC-E specification pdf. 

import datagen as dg
import numpy as np
import pandas as pd
import random
import datetime
simulation_start_date = datetime.datetime.now()

#############################Market Exchange Functions#########################
def nowTime():
    base_time = max(list(dg.TradeHistory['TH_DTS']))
    base_time = datetime.datetime.strptime(base_time,"%Y-%m-%d %H:%M:%S.%f")
    current_time = datetime.datetime.now()
    current_time = current_time - simulation_start_date + base_time
    return current_time

def marketPrice(symbol,trade_start):
     msPerPeriod = 900000000
     securityIndex = dg.Security.loc[dg.Security['S_SYMB'] == symbol].index.values[0]
     securityFactor =securityIndex*556237 + 253791
     trading_time = dg.LastTrade.loc[dg.LastTrade['LT_S_SYMB']==symbol,'LT_DTS'].values[0] 
     trading_time = datetime.datetime.strptime(trading_time,"%Y-%m-%d %H:%M:%S.%f")
     current_time = nowTime()
     timesofar = current_time - trading_time
     timesofar = timesofar.seconds * 1000000 + timesofar.microseconds
     initialtime = (timesofar + securityFactor) % msPerPeriod
     initialtime = initialtime / 1000000
     ftime = current_time - trade_start
     ftime = ftime.seconds
     fperiodtime = (ftime + initialtime)/900
     ftimeinperiod = (fperiodtime - int(fperiodtime))*900
     if ftimeinperiod < (900/2):
         fPricePosition = ftimeinperiod / (900 / 2)
     else:
        fPricePosition = (900 - ftimeinperiod) / (900/ 2)
     price = 20 + 10*fPricePosition
     return price
    

##########################profile creation#####################################
def createProfile(profile,trans,access_sequence =''):
    URV = ['o' for i in range(dg.URV_size)]
    if access_sequence == '':
        access_sequence = list(profile.keys())
    for feature in profile.keys():
        if dg.URV_feature_type[feature] == 'C':
            print(feature)
            URV[dg.URV_feature_index[feature]] = len(set(profile[feature]))
            URV[dg.URV_feature_index[feature] + 1] = len(profile[feature])
        else:
            try:
                URV[dg.URV_feature_index[feature]] = np.mean(profile[feature])
                URV[dg.URV_feature_index[feature]+1] = np.median(profile[feature])
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

############################MARKET FEED TRANSACTION############################
def marketfeed():
    profile = {}
    trade_start = min(list(dg.TradeHistory['TH_DTS']))
    trade_start = datetime.datetime.strptime(trade_start,"%Y-%m-%d %H:%M:%S.%f")
    current_time = nowTime()
    current_time_string = datetime.datetime.strftime(current_time,"%Y-%m-%d %H:%M:%S.%f")
    traderequest = dg.TradeRequest[['TR_S_SYMB','TR_QTY']]
    security = list(traderequest['TR_S_SYMB'])
    trade_qty = list(traderequest['TR_QTY'])
    '''security = random.sample(list(dg.Security['S_SYMB']),20)
    trade_qty = [np.random.randint(1,6) for i in range(20)]'''
    price_quote = []
    cntr = 0
    profile['LT_S_SYMB'] = []
    profile['LT_PRICE'] = []
    profile['LT_VOL'] = []
    profile['LT_DTS'] = []
    profile['TR_S_SYMB'] = security
    profile['TR_T_ID'] = list(dg.TradeRequest['TR_T_ID'])
    profile['TR_BID_PRICE'] = list(dg.TradeRequest['TR_BID_PRICE']) 
    profile['TR_TT_ID'] = list(dg.TradeRequest['TR_TT_ID'])
    profile['TR_QTY'] = trade_qty
    profile['TR_T_ID'] = []
    profile['T_DTS'] = []
    profile['T_ST_ID'] = []
    profile["TH_T_ID"] = []
    profile["TH_DTS"] = []
    profile["TH_ST_ID"] = []
    for symbol in security:
        price = marketPrice(symbol, trade_start)
        price_quote.append(price)
        index = dg.LastTrade['LT_S_SYMB']==symbol
        dg.LastTrade.loc[index,'LT_PRICE'] = price
        dg.LastTrade.loc[index,'LT_VOL'] += trade_qty[cntr]
        dg.LastTrade.loc[index,'LT_DTS'] = current_time_string
        profile['LT_S_SYMB'].append(symbol)
        profile['LT_PRICE'].append(price)
        profile['LT_VOL'].append(dg.LastTrade.loc[index,'LT_VOL'].values[0])
        profile['LT_DTS'].append(current_time_string)
        tradehappen = dg.TradeRequest.loc[(dg.TradeRequest['TR_S_SYMB']==symbol) & (
        ((dg.TradeRequest['TR_TT_ID']=='TSL')&(dg.TradeRequest['TR_BID_PRICE']>=price))
        | ((dg.TradeRequest['TR_TT_ID']=='TLS')&(dg.TradeRequest['TR_BID_PRICE']<=price))
        | ((dg.TradeRequest['TR_TT_ID']=='TLB')&(dg.TradeRequest['TR_BID_PRICE']>=price))
        ),['TR_T_ID','TR_BID_PRICE','TR_TT_ID','TR_QTY']]
        if tradehappen.shape[0] != 0 :
            trade_id = tradehappen['TR_T_ID'].values[0]
            index = dg.Trade['T_ID'] == trade_id
            dg.Trade.loc[index,'T_DTS'] = current_time_string
            dg.Trade.loc[index,'T_ST_ID'] = 'SBMT'
            profile['TR_T_ID'].append(trade_id)
            profile['T_DTS'].append(current_time_string)
            profile['T_ST_ID'].append('SBMT')
            history = pd.DataFrame({"TH_T_ID":[trade_id],"TH_DTS":[current_time_string],"TH_ST_ID":['SBMT']})
            profile["TH_T_ID"].append(trade_id)
            profile["TH_DTS"].append(current_time_string)
            profile["TH_ST_ID"].append('SBMT')
            dg.TradeHistory = dg.TradeHistory.append(history)
            request_index = tradehappen.index.values[0]
            dg.TradeRequest.drop(index = request_index,inplace = True)
        cntr += 1
        
    createProfile(profile, " marketfeed")
    return len(security)
    
 ############################MARKET WATCH TRANSACTION###########################
def marketwatch(acct_id=0,cust_id=0,industry_name='',ending_co_id=0,starting_co_id=0,start_date=''):
    profile = {}
    if acct_id==0 and cust_id==0 and industry_name=='':
        sel = np.random.uniform(0,1,1)
        if sel<0.35:
            group = np.random.randint(2)
            if group == 0:
                customers = list(dg.Customer['C_ID'])
            else:
                tier = np.random.randint(1,4)
                customers = list(dg.Customer.loc[dg.Customer['C_TIER']==tier,'C_ID'])
            cust_id = customers[np.random.randint(len(customers))]
            accounts = list(dg.CustomerAccount.loc[dg.CustomerAccount['CA_C_ID']==cust_id,'CA_ID'])
            acct_id = accounts[np.random.randint(len(accounts))]
        elif sel<0.95:
            group = np.random.randint(2)
            if group == 0:
                customers = list(dg.Customer['C_ID'])
            else:
                tier = np.random.randint(1,4)
                customers = list(dg.Customer.loc[dg.Customer['C_TIER']==tier,'C_ID'])
            cust_id = customers[np.random.randint(len(customers))]
        else:
            industries = list(dg.Industry['IN_NAME'])
            industry_name = industries[np.random.randint(len(industries))]
            
    if acct_id !=0:
        stock_list = list(dg.HoldingSummary.loc[dg.HoldingSummary['HS_CA_ID']==acct_id,'HS_S_SYMB'])
        profile['HS_CA_ID'] = [acct_id for i in range(len(stock_list))]
        profile['HS_S_SYMB'] = stock_list
    elif cust_id !=0:
        watch_id = dg.WatchList.loc[dg.WatchList['WL_C_ID']==cust_id,'WL_ID'].values[0]
        stock_list = list(dg.WatchItem.loc[dg.WatchItem['WI_WL_ID']==watch_id,'WI_S_SYMB'])
        profile['WL_C_ID'] = [cust_id]
        profile['WL_ID'] = [watch_id]
        profile['WI_WL_ID'] = [watch_id for i in range(len(stock_list))]
        profile['WI_S_SYMB'] = stock_list
    elif industry_name!='':
        industry_id = dg.Industry.loc[dg.Industry['IN_NAME']==industry_name,'IN_ID'].values[0]
        profile['IN_NAME'] = industry_name
        profile['IN_ID'] = industry_id
        company_id = list(dg.Company.loc[dg.Company['CO_IN_ID']==industry_id,'CO_ID'])
        if ending_co_id!=0:
            company_id = np.array(company_id)
            company_id = company_id[(company_id >= starting_co_id) & (company_id<= ending_co_id)]
            company_id = list(company_id)
        profile['CO_IN_ID'] = [industry_id for i in range(len(company_id))]
        profile['CO_ID'] = company_id
        security = dg.Security.loc[dg.Security['S_CO_ID'].isin(company_id),['S_CO_ID','S_SYMB']]
        stock_list = list(security['S_SYMB'])
        profile['S_CO_ID'] = list(security['S_CO_ID'])
    
    profile['S_SYMB'] = stock_list
    profile['LT_PRICE'] = []
    profile['LT_S_SYMB'] = stock_list
    profile['S_NUM_OUT'] = []
    profile['DM_DATE'] = []
    profile['DM_S_SYMB'] = stock_list
    profile['DM_CLOSE'] = []
    
    old_mkt_cap = 0.00
    new_mkt_cap = 0.00
    pct_change = 0.00
    
    for symbol in stock_list:
        try:
            new_price = dg.LastTrade.loc[dg.LastTrade['LT_S_SYMB'] == symbol,'LT_PRICE'].values[0]
            s_num_out = dg.Security.loc[dg.Security['S_SYMB'] == symbol,'S_NUM_OUT'].values[0]
            if start_date == '':
                date_index = 1 + int(np.random.exponential(1))
                if date_index > 1305 :
                    date_index = 1305
                date = list(dg.DailyMarket['DM_DATE'])
                start_date = date[0-date_index]
            old_price = dg.DailyMarket.loc[(dg.DailyMarket['DM_DATE'] == start_date) & (dg.DailyMarket['DM_S_SYMB'] == symbol),'DM_CLOSE'].values[0]
            old_mkt_cap += s_num_out * old_price
            new_mkt_cap += s_num_out * new_price
            profile['LT_PRICE'].append(new_price)
            profile['S_NUM_OUT'].append(s_num_out)
            profile['DM_DATE'].append(start_date)
            profile['DM_CLOSE'].append(old_price)
        except:
            old_mkt_cap = 0.00
    if old_mkt_cap != 0.00:
        pct_change = 100 * (new_mkt_cap / old_mkt_cap - 1)
        
    createProfile(profile, ' marketwatch ' + str(cust_id) + '' + str(acct_id) )
    return acct_id,cust_id,industry_name,ending_co_id,starting_co_id,start_date,pct_change

############################SECURITY DETAIL TRANSACTION########################
def securitydetail(access_lob_flag = '',max_rows_to_return = '',start_day = '',symbol = ''):
    profile = {}
    if symbol == '':
        security = list(dg.Security['S_SYMB'])
        symbol = security[np.random.randint(len(security))]
    security = dg.Security.loc[dg.Security['S_SYMB'] == symbol,['S_CO_ID',
   'S_NAME','S_NUM_OUT','S_START_DATE','S_EXCH_DATE','S_PE','S_52WK_HIGH',
   'S_52WK_HIGH_DATE','S_52WK_LOW','S_52WK_LOW_DATE','S_DIVIDEND','S_YIELD','S_EX_ID']]
    profile['S_SYMB'] = [symbol]
    profile['S_CO_ID'] = [security['S_CO_ID'].values[0]]
    profile['S_NAME'] = [security['S_NAME'].values[0]]
    profile['S_NUM_OUT'] = [security['S_NUM_OUT'].values[0]]
    profile['S_START_DATE'] = [security['S_START_DATE'].values[0]]
    profile['S_EXCH_DATE'] = [security['S_EXCH_DATE'].values[0]]
    profile['S_PE'] = [security['S_PE'].values[0]]
    profile['S_52WK_HIGH'] = [security['S_52WK_HIGH'].values[0]]
    profile['S_52WK_HIGH_DATE'] = [security['S_52WK_HIGH_DATE'].values[0]]
    profile['S_52WK_LOW'] = [security['S_52WK_LOW'].values[0]]
    profile['S_52WK_LOW_DATE'] = [security['S_52WK_LOW_DATE'].values[0]]
    profile['S_DIVIDEND'] = [security['S_DIVIDEND'].values[0]]
    profile['S_YIELD'] = [security['S_YIELD'].values[0]]
    profile['S_EX_ID'] = [security['S_EX_ID'].values[0]]
    company = dg.Company.loc[dg.Company['CO_ID'] == int(security['S_CO_ID']),[
    'CO_ID','CO_AD_ID','CO_NAME','CO_SP_RATE','CO_CEO','CO_DESC','CO_OPEN_DATE',
    'CO_ST_ID']]
    profile['CO_ID'] = [company['CO_ID'].values[0]]
    profile['CO_AD_ID'] = [company['CO_AD_ID'].values[0]]
    profile['CO_NAME'] = [company['CO_NAME'].values[0]]
    profile['CO_SP_RATE'] = [company['CO_SP_RATE'].values[0]]
    profile['CO_CEO'] = [company['CO_CEO'].values[0]]
    profile['CO_DESC'] = [company['CO_DESC'].values[0]]
    profile['CO_OPEN_DATE'] = [company['CO_OPEN_DATE'].values[0]]
    profile['CO_ST_ID'] = [company['CO_ST_ID'].values[0]]
    exchange = dg.Exchange.loc[dg.Exchange['EX_ID'] == security['S_EX_ID'].values[0],
    ['EX_AD_ID','EX_CLOSE','EX_DESC','EX_NAME','EX_NUM_SYMB','EX_OPEN']]
    profile['EX_ID'] = [security['S_EX_ID'].values[0]]
    profile['EX_AD_ID'] = [exchange['EX_AD_ID'].values[0]]
    profile['EX_CLOSE'] = [exchange['EX_CLOSE'].values[0]]
    profile['EX_DESC'] = [exchange['EX_DESC'].values[0]]
    profile['EX_NAME'] = [exchange['EX_NAME'].values[0]]
    profile['EX_NUM_SYMB'] = [exchange['EX_NUM_SYMB'].values[0]]
    profile['EX_OPEN'] = [exchange['EX_OPEN'].values[0]]
    ca_address = dg.Address.loc[dg.Address['AD_ID'] == company['CO_AD_ID'].values[0],
    ["AD_ID", "AD_LINE1","AD_LINE2","AD_ZC_CODE","AD_CTRY"]]
    profile["AD_ID"] = [ca_address["AD_ID"].values[0]]
    profile["AD_LINE1"] = [ca_address["AD_LINE1"].values[0]]
    profile["AD_LINE2"] = [ca_address["AD_LINE2"].values[0]]
    profile["AD_ZC_CODE"] = [ca_address["AD_ZC_CODE"].values[0]]
    profile["AD_CTRY"] = [ca_address["AD_CTRY"].values[0]]
    ea_address = dg.Address.loc[dg.Address['AD_ID'] == exchange['EX_AD_ID'].values[0],
    ["AD_ID", "AD_LINE1","AD_LINE2","AD_ZC_CODE","AD_CTRY"]]
    profile["AD_ID"].append(ea_address["AD_ID"].values[0])
    profile["AD_LINE1"].append(ea_address["AD_LINE1"].values[0])
    profile["AD_LINE2"].append(ea_address["AD_LINE2"].values[0])
    profile["AD_ZC_CODE"].append(ea_address["AD_ZC_CODE"].values[0])
    profile["AD_CTRY"].append(ea_address["AD_CTRY"].values[0])
    zca = dg.ZipCode.loc[dg.ZipCode['ZC_CODE'] == ca_address['AD_ZC_CODE'].values[0],
    ["ZC_CODE", "ZC_TOWN","ZC_DIV"]]
    profile["ZC_CODE"] = [zca["ZC_CODE"].values[0]]
    profile["ZC_TOWN"] = [zca["ZC_TOWN"].values[0]]
    profile["ZC_DIV"] = [zca["ZC_DIV"].values[0]]
    zea = dg.ZipCode.loc[dg.ZipCode['ZC_CODE'] == ea_address['AD_ZC_CODE'].values[0],
    ["ZC_CODE", "ZC_TOWN","ZC_DIV"]]
    profile["ZC_CODE"].append(zea["ZC_CODE"].values[0])
    profile["ZC_TOWN"].append(zea["ZC_TOWN"].values[0])
    profile["ZC_DIV"].append(zea["ZC_DIV"].values[0])
    competitor = dg.CompanyCompetitor.loc[dg.CompanyCompetitor['CP_CO_ID'] == 
    company['CO_ID'].values[0],["CP_CO_ID", "CP_COMP_CO_ID","CP_IN_ID"]]
    profile["CP_CO_ID"] = list(competitor["CP_CO_ID"].values)
    profile["CP_COMP_CO_ID"] = list(competitor["CP_COMP_CO_ID"].values)
    profile["CP_IN_ID"] = list(competitor["CP_IN_ID"].values)
    cp_co_name = []
    cp_in_name = []
    for row in competitor.iterrows():
        co_name = dg.Company.loc[dg.Company['CO_ID'] == row[1]['CP_COMP_CO_ID'],'CO_NAME'].values[0]
        in_name = dg.Industry.loc[dg.Industry['IN_ID'] == row[1]['CP_IN_ID'],'IN_NAME'].values[0]
        cp_co_name.append(co_name)
        cp_in_name.append(in_name)
    profile['CO_ID'].extend(list(competitor["CP_COMP_CO_ID"].values))
    profile['CO_NAME'] = cp_co_name
    profile['IN_ID'] = list(competitor["CP_IN_ID"].values)
    profile['IN_NAME'] = cp_in_name
    financial = dg.Financial.loc[dg.Financial['FI_CO_ID']==company['CO_ID'].values[0],
    ["FI_CO_ID","FI_YEAR","FI_QTR","FI_OTR_START_DATE","FI_REVENUE","FI_NET_EARN",
    "FI_BASIC_EPS","FI_DILUT_EPS","FI_MARGIN","FI_INVENTORY","FI_ASSETS","FI_LIABILITY"
    ,"FI_OUT_BASIC","FI_OUT_DILUT"]]
    financial = financial[:20]
    profile["FI_CO_ID" ]= list(financial["FI_CO_ID"].values)
    profile["FI_YEAR"] = list(financial["FI_YEAR"].values)
    profile["FI_QTR"] = list(financial["FI_QTR"].values)
    profile["FI_OTR_START_DATE"] = list(financial["FI_OTR_START_DATE"].values)
    profile["FI_REVENUE"] = list(financial["FI_REVENUE"].values)
    profile["FI_NET_EARN"] = list(financial["FI_NET_EARN"].values)
    profile["FI_BASIC_EPS"] = list(financial["FI_BASIC_EPS"].values)
    profile["FI_DILUT_EPS"] = list(financial["FI_DILUT_EPS"].values)
    profile["FI_MARGIN"] = list(financial["FI_MARGIN"].values)
    profile["FI_INVENTORY"] = list(financial["FI_INVENTORY"].values)
    profile["FI_ASSETS"] = list(financial["FI_ASSETS"].values)
    profile["FI_LIABILITY"] = list(financial["FI_LIABILITY"].values)
    profile["FI_OUT_BASIC"] = list(financial["FI_OUT_BASIC"].values)
    profile["FI_OUT_DILUT"] = list(financial["FI_OUT_DILUT"].values)
    if max_rows_to_return == '':
        max_rows_to_return = np.random.randint(5,21)
    if start_day == '':
        date = list(dg.DailyMarket['DM_DATE'])
        start_day = date[np.random.randint(len(date)-max_rows_to_return)]
    dailymarket = dg.DailyMarket.loc[(dg.DailyMarket['DM_S_SYMB'] == symbol) 
    & (dg.DailyMarket['DM_DATE']>=start_day),['DM_S_SYMB','DM_DATE','DM_CLOSE','DM_HIGH',
   'DM_LOW','DM_VOL']].head(max_rows_to_return)
    profile['DM_S_SYMB'] = list(dailymarket['DM_S_SYMB'].values)
    profile['DM_DATE'] = list(dailymarket['DM_DATE'].values)
    profile['DM_CLOSE'] = list(dailymarket['DM_CLOSE'].values)
    profile['DM_HIGH'] = list(dailymarket['DM_HIGH'].values)
    profile['DM_LOW'] = list(dailymarket['DM_LOW'].values)
    profile['DM_VOL'] = list(dailymarket['DM_VOL'].values)
    lasttrade = dg.LastTrade.loc[dg.LastTrade['LT_S_SYMB']==symbol,['LT_S_SYMB','LT_PRICE',
    'LT_OPEN_PRICE','LT_VOL']]
    profile['LT_S_SYMB'] = list(lasttrade['LT_S_SYMB'].values)
    profile['LT_PRICE'] = list(lasttrade['LT_PRICE'].values)
    profile['LT_OPEN_PRICE'] = list(lasttrade['LT_OPEN_PRICE'].values)
    profile['LT_VOL'] = list(lasttrade['LT_VOL'].values)
    if access_lob_flag == '':
        prob = random.uniform(0,1)
        if prob < 0.99 :
            access_lob_flag = 0
        else:
            access_lob_flag = 1
    if access_lob_flag == 1:
        newsxref = dg.NewsXRef.loc[dg.NewsXRef['NX_CO_ID'] == company['CO_ID'].values[0],
        ['NX_CO_ID','NX_NI_ID']].head(2)
        profile['NX_CO_ID'] = list(newsxref['NX_CO_ID'].values)
        profile['NX_NI_ID'] = list(newsxref['NX_NI_ID'].values)
        newsitem = dg.NewsItem.loc[dg.NewsItem['NI_ID'].isin(list(newsxref['NX_NI_ID'])),
        ['NI_ID','NI_ITEM','NI_DTS','NI_SOURCE','NI_AUTHOR']]
        profile['NI_ID'] = list(newsitem['NI_ID'].values)
        profile['NI_ITEM'] = list(newsitem['NI_ITEM'].values)
        profile['NI_DTS'] = list(newsitem['NI_DTS'].values)
        profile['NI_SOURCE'] = list(newsitem['NI_SOURCE'].values)
        profile['NI_AUTHOR'] = list(newsitem['NI_AUTHOR'].values)
    else:
        newsxref = dg.NewsXRef.loc[dg.NewsXRef['NX_CO_ID'] == company['CO_ID'].values[0],
        ['NX_CO_ID','NX_NI_ID']].head(2)
        profile['NX_CO_ID'] = list(newsxref['NX_CO_ID'].values)
        profile['NX_NI_ID'] = list(newsxref['NX_NI_ID'].values)
        newsitem = dg.NewsItem.loc[dg.NewsItem['NI_ID'].isin(list(newsxref['NX_NI_ID'])),
        ['NI_ID','NI_DTS','NI_SOURCE','NI_AUTHOR','NI_HEADLINE','NI_SUMMARY']]
        profile['NI_ID'] = list(newsitem['NI_ID'].values)
        profile['NI_DTS'] = list(newsitem['NI_DTS'].values)
        profile['NI_SOURCE'] = list(newsitem['NI_SOURCE'].values)
        profile['NI_AUTHOR'] = list(newsitem['NI_AUTHOR'].values)
        profile['NI_HEADLINE'] = list(newsitem['NI_HEADLINE'].values)
        profile['NI_SUMMARY'] = list(newsitem['NI_SUMMARY'].values)
        
    createProfile(profile, " securitydetail")
    
    return access_lob_flag,start_day,symbol,max_rows_to_return,security,company,
    exchange,ca_address,ea_address,zca,zea,cp_co_name,cp_in_name,financial,dailymarket,
    lasttrade,newsitem

###############################TRADE LOOKUP TRANSACTION########################
def tradelookup(acct_id=0,end_trade_dts='',frame_to_execute='',max_acct_id=0,max_trades=0,start_trade_dts='',symbol='',trade_id=[]):
    if frame_to_execute=='':
        prob=random.uniform(0,1)
        if prob < 0.30:
            frame_to_execute = 1
        elif prob < 0.60:
            frame_to_execute = 2
        elif prob < 0.90:
            frame_to_execute = 3
        else:
            frame_to_execute = 4

    profile = {}
    if frame_to_execute==1:
        if max_trades == 0:
            max_trades = 20
            trade_id=random.sample(list(dg.Trade['T_ID']),max_trades)
        trade=dg.Trade.loc[dg.Trade['T_ID'].isin(trade_id),['T_ID','T_BID_PRICE',
        'T_EXEC_NAME','T_IS_CASH','T_TRADE_PRICE','T_TT_ID']]
        trade_type = dg.TradeType.loc[dg.TradeType['TT_ID'].isin(list(trade['T_TT_ID'])),
        ['TT_ID','TT_IS_MRKT']]
        trade = trade.set_index('T_TT_ID',drop = False).join(trade_type.set_index('TT_ID',drop = False))
        profile['T_ID'] = list(trade['T_ID'].values)
        profile['T_BID_PRICE'] = list(trade['T_BID_PRICE'].values)
        profile['T_EXEC_NAME'] = list(trade['T_EXEC_NAME'].values)
        profile['T_IS_CASH'] = list(trade['T_IS_CASH'].values)
        profile['T_TRADE_PRICE'] = list(trade['T_TRADE_PRICE'].values)
        profile['T_TT_ID'] = list(trade['T_TT_ID'].values)
        profile['TT_ID'] = list(trade['TT_ID'].values)
        profile['TT_IS_MRKT'] = list(trade['TT_IS_MRKT'].values)
        settlement=dg.Settlement.loc[dg.Settlement['SE_T_ID'].isin(trade_id),
        ['SE_AMT','SE_CASH_DUE_DATE','SE_CASH_TYPE']]
        profile['SE_T_ID'] = trade_id
        profile['SE_AMT'] = list(settlement['SE_AMT'].values)
        profile['SE_CASH_DUE_DATE'] = list(settlement['SE_CASH_DUE_DATE'].values)
        profile['SE_CASH_TYPE'] = list(settlement['SE_CASH_TYPE'].values)
        cash = pd.DataFrame()
        for row in trade.iterrows():
            tid = row[1]['T_ID']
            tcash = row[1]['T_IS_CASH']
            if tcash==1:
                iscash = dg.CashTransaction.loc[dg.CashTransaction['CT_T_ID']==tid,['CT_T_ID','CT_AMT','CT_DTS','CT_NAME']]
                cash = cash.append(iscash)
        profile['CT_T_ID'] = list(cash['CT_T_ID'].values)
        profile['CT_AMT'] = list(cash['CT_AMT'].values)
        profile['CT_DTS'] = list(cash['CT_DTS'].values)
        profile['CT_NAME'] = list(cash['CT_NAME'].values)
        tradehistory = dg.TradeHistory.loc[dg.TradeHistory['TH_T_ID'].isin(trade_id),['TH_T_ID','TH_DTS','TH_ST_ID']]
        profile['TH_T_ID'] = list(tradehistory['TH_T_ID'].values)
        profile['TH_DTS'] = list(tradehistory['TH_DTS'].values)
        profile['TH_ST_ID'] = list(tradehistory['TH_ST_ID'].values) 
        
        createProfile(profile," tradelookup " + str(1))
        return frame_to_execute , max_trades , trade_id , trade , settlement , cash , tradehistory

    elif frame_to_execute == 2:
        if max_trades == 0:
            max_trades = 20
            group = np.random.randint(2)
            if group == 0:
                customers = list(dg.Customer['C_ID'])
            else:
                tier = np.random.randint(1,4)
                customers = list(dg.Customer.loc[dg.Customer['C_TIER']==tier,'C_ID'])
            cust_id = customers[np.random.randint(len(customers))]
            accounts = list(dg.CustomerAccount.loc[dg.CustomerAccount['CA_C_ID']==cust_id,'CA_ID'])
            acct_id = accounts[np.random.randint(len(accounts))]
            date=random.sample(list(dg.Trade['T_DTS']),2)
            start_trade_dts = min(date)
            end_trade_dts = max(date)
        trade = dg.Trade.loc[(dg.Trade['T_DTS']>=start_trade_dts) & 
        (dg.Trade['T_DTS']<=end_trade_dts) & (dg.Trade['T_CA_ID']==acct_id) ,
        ['T_CA_ID','T_BID_PRICE','T_EXEC_NAME','T_IS_CASH','T_ID','T_TRADE_PRICE'
         ,'T_DTS']]
        trade = trade[:max_trades]
        trade_id = list(trade['T_ID'])
        profile['T_DTS'] = list(trade['T_DTS'].values)
        profile['T_CA_ID'] = list(trade['T_CA_ID'].values)
        profile['T_BID_PRICE'] = list(trade['T_BID_PRICE'].values)
        profile['T_EXEC_NAME'] = list(trade['T_EXEC_NAME'].values)
        profile['T_IS_CASH'] = list(trade['T_IS_CASH'].values)
        profile['T_ID'] = list(trade['T_ID'].values)
        profile['T_TRADE_PRICE'] = list(trade['T_TRADE_PRICE'].values)
        settlement=dg.Settlement.loc[dg.Settlement['SE_T_ID'].isin(trade_id),
        ['SE_AMT','SE_CASH_DUE_DATE','SE_CASH_TYPE']]
        profile['SE_T_ID'] = trade_id
        profile['SE_AMT'] = list(settlement['SE_AMT'].values)
        profile['SE_CASH_DUE_DATE'] = list(settlement['SE_CASH_DUE_DATE'].values)
        profile['SE_CASH_TYPE'] = list(settlement['SE_CASH_TYPE'].values)
        cash = pd.DataFrame()
        for row in trade.iterrows():
            tid = row[1]['T_ID']
            tcash = row[1]['T_IS_CASH']
            if tcash==1:
                iscash = dg.CashTransaction.loc[dg.CashTransaction['CT_T_ID']==tid,['CT_T_ID','CT_AMT','CT_DTS','CT_NAME']]
                cash = cash.append(iscash)
        profile['CT_T_ID'] = list(cash['CT_T_ID'].values)
        profile['CT_AMT'] = list(cash['CT_AMT'].values)
        profile['CT_DTS'] = list(cash['CT_DTS'].values)
        profile['CT_NAME'] = list(cash['CT_NAME'].values)
        tradehistory = dg.TradeHistory.loc[dg.TradeHistory['TH_T_ID'].isin(trade_id),['TH_T_ID','TH_DTS','TH_ST_ID']]
        profile['TH_T_ID'] = list(tradehistory['TH_T_ID'].values)
        profile['TH_DTS'] = list(tradehistory['TH_DTS'].values)
        profile['TH_ST_ID'] = list(tradehistory['TH_ST_ID'].values)
        
        createProfile(profile," tradelookup " + str(2) + " " + str(acct_id))
        return frame_to_execute , max_trades , acct_id , start_trade_dts , end_trade_dts, trade , settlement , cash , tradehistory

    elif frame_to_execute == 3:
        if max_trades==0:
            max_trades = 20
            security = list(dg.Security['S_SYMB'])
            symbol = security[np.random.randint(len(security))] 
            date=random.sample(list(dg.Trade['T_DTS']),2)
            start_trade_dts = min(date)
            end_trade_dts = max(date)
        trade = dg.Trade.loc[(dg.Trade['T_DTS']>=start_trade_dts) & 
        (dg.Trade['T_DTS']<=end_trade_dts) & (dg.Trade['T_S_SYMB']==symbol) ,
        ['T_S_SYMB','T_CA_ID','T_EXEC_NAME','T_IS_CASH','T_ID','T_TRADE_PRICE',
         'T_QTY','T_TT_ID','T_DTS']]
        trade = trade[:max_trades]
        trade_id = list(trade['T_ID'])
        profile['T_DTS'] = list(trade['T_DTS'].values)
        profile['T_S_SYMB'] = list(trade['T_S_SYMB'].values)
        profile['T_CA_ID'] = list(trade['T_CA_ID'].values)
        profile['T_EXEC_NAME'] = list(trade['T_EXEC_NAME'].values)
        profile['T_IS_CASH'] = list(trade['T_IS_CASH'].values)
        profile['T_ID'] = list(trade['T_ID'].values)
        profile['T_TRADE_PRICE'] = list(trade['T_TRADE_PRICE'].values)
        profile['T_QTY'] = list(trade['T_QTY'].values)
        profile['T_TT_ID'] = list(trade['T_TT_ID'].values)
        settlement=dg.Settlement.loc[dg.Settlement['SE_T_ID'].isin(trade_id),
        ['SE_AMT','SE_CASH_DUE_DATE','SE_CASH_TYPE']]
        profile['SE_T_ID'] = trade_id
        profile['SE_AMT'] = list(settlement['SE_AMT'].values)
        profile['SE_CASH_DUE_DATE'] = list(settlement['SE_CASH_DUE_DATE'].values)
        profile['SE_CASH_TYPE'] = list(settlement['SE_CASH_TYPE'].values)
        cash = pd.DataFrame()
        for row in trade.iterrows():
            tid = row[1]['T_ID']
            tcash = row[1]['T_IS_CASH']
            if tcash==1:
                iscash = dg.CashTransaction.loc[dg.CashTransaction['CT_T_ID']==tid,['CT_T_ID','CT_AMT','CT_DTS','CT_NAME']]
                cash = cash.append(iscash)
        profile['CT_T_ID'] = list(cash['CT_T_ID'].values)
        profile['CT_AMT'] = list(cash['CT_AMT'].values)
        profile['CT_DTS'] = list(cash['CT_DTS'].values)
        profile['CT_NAME'] = list(cash['CT_NAME'].values)
        tradehistory = dg.TradeHistory.loc[dg.TradeHistory['TH_T_ID'].isin(trade_id),['TH_T_ID','TH_DTS','TH_ST_ID']]
        profile['TH_T_ID'] = list(tradehistory['TH_T_ID'].values)
        profile['TH_DTS'] = list(tradehistory['TH_DTS'].values)
        profile['TH_ST_ID'] = list(tradehistory['TH_ST_ID'].values)
        
        createProfile(profile," tradelookup " + str(3))
        return frame_to_execute , max_trades , symbol , start_trade_dts , end_trade_dts, trade , settlement , cash , tradehistory
    
    elif frame_to_execute==4:
        if max_trades == 0:
            max_trades = 20
            group = np.random.randint(2)
            if group == 0:
                customers = list(dg.Customer['C_ID'])
            else:
                tier = np.random.randint(1,4)
                customers = list(dg.Customer.loc[dg.Customer['C_TIER']==tier,'C_ID'])
            cust_id = customers[np.random.randint(len(customers))]
            accounts = list(dg.CustomerAccount.loc[dg.CustomerAccount['CA_C_ID']==cust_id,'CA_ID'])
            acct_id = accounts[np.random.randint(len(accounts))]
            date=random.sample(list(dg.Trade['T_DTS']),2)
            start_trade_dts = date[0]
        trade_id = dg.Trade.loc[(dg.Trade['T_DTS']>=start_trade_dts) & 
        (dg.Trade['T_CA_ID']==acct_id) ,'T_ID'][:1].values[0]
        profile['T_CA_ID'] = [acct_id]
        profile['T_DTS'] = [start_trade_dts]
        profile['T_ID'] = [trade_id]
        holdinghistory_id = dg.HoldingHistory.loc[dg.HoldingHistory['HH_T_ID']==trade_id,'HH_H_T_ID'].values[0]
        holdinghistory = dg.HoldingHistory.loc[dg.HoldingHistory['HH_H_T_ID']==holdinghistory_id,['HH_H_T_ID',
         'HH_T_ID','HH_BEFORE_QTY','HH_AFTER_QTY']][:max_trades]
        profile['HH_T_ID'] = list(holdinghistory['HH_T_ID'].values)
        profile['HH_H_T_ID'] = list(holdinghistory['HH_H_T_ID'].values)
        profile['HH_BEFORE_QTY'] = list(holdinghistory['HH_BEFORE_QTY'].values)
        profile['HH_AFTER_QTY'] = list(holdinghistory['HH_AFTER_QTY'].values)
        
        createProfile(profile," tradelookup " + str(2) + " " + str(acct_id))
        return frame_to_execute , max_trades , acct_id , start_trade_dts , trade_id , holdinghistory

###############################TRADE ORDER TRANSACTION#########################
def tradeorder(acct_id = '',exec_f_name='',exec_l_name='',exec_tax_id='',is_lifo='',co_name ='',issue='',symbol='',trade_type_id='', trade_qty='',type_is_margin='',roll_it_back='',requested_price=''):

    profile = {}
    if acct_id == '':
         group = np.random.randint(2)
         if group == 0:
             customers = list(dg.Customer['C_ID'])
         else:
            tier = np.random.randint(1,4)
            customers = list(dg.Customer.loc[dg.Customer['C_TIER']==tier,'C_ID'])
         cust_id = customers[np.random.randint(len(customers))]
         accounts = list(dg.CustomerAccount.loc[dg.CustomerAccount['CA_C_ID']==cust_id,'CA_ID'])
         acct_id = accounts[np.random.randint(len(accounts))]
    account = dg.CustomerAccount.loc[dg.CustomerAccount['CA_ID']==acct_id,
    ['CA_NAME','CA_C_ID','CA_B_ID','CA_TAX_ST']]
    tax_status = account['CA_TAX_ST'].values[0]
    profile['CA_ID'] = [acct_id]
    profile['CA_NAME'] = list(account['CA_NAME'].values)
    profile['CA_C_ID'] = list(account['CA_C_ID'].values)
    profile['CA_B_ID'] = list(account['CA_B_ID'].values)
    profile['CA_TAX_ST'] = list(account['CA_TAX_ST'].values)
    cust_id = account['CA_C_ID'].values[0]
    broker_id = account['CA_B_ID'].values[0]
    customer = dg.Customer.loc[dg.Customer['C_ID']==cust_id,['C_F_NAME',
    'C_L_NAME','C_TIER','C_TAX_ID']]
    cust_tier = customer['C_TIER'].values[0]
    profile['C_ID'] = [cust_id]
    profile['C_F_NAME'] = list(customer['C_F_NAME'].values)
    profile['C_L_NAME'] = list(customer['C_L_NAME'].values)
    profile['C_TIER'] = list(customer['C_TIER'].values)
    profile['C_TAX_ID'] = list(customer['C_TAX_ID'].values)
    broker = dg.Broker.loc[dg.Broker['B_ID']==broker_id,'B_NAME']
    profile['B_ID'] = [broker_id]
    profile['B_NAME'] = list(broker.values)
    if exec_f_name == '':
        prob = random.uniform(0,1)
        if prob < 0.15:
            executioner = dg.AccountPermission.loc[dg.AccountPermission['AP_CA_ID']==acct_id,
            ['AP_F_NAME','AP_L_NAME','AP_TAX_ID']].values
            size = len(executioner)
            pos = np.random.randint(size)
            exec_f_name = executioner[pos][0]
            exec_l_name = executioner[pos][1]
            exec_tax_id = executioner[pos][2]
        else:
            exec_f_name = customer['C_F_NAME'].values[0]
            exec_l_name = customer['C_L_NAME'].values[0]
            exec_tax_id = customer['C_TAX_ID'].values[0]
    
    if ((exec_f_name != customer['C_F_NAME'].values[0])
    or (exec_l_name != customer['C_L_NAME'].values[0])
    or (exec_tax_id != customer['C_TAX_ID'].values[0])):
        executioner = dg.AccountPermission.loc[
        (dg.AccountPermission['AP_CA_ID']==acct_id)
        &(dg.AccountPermission['AP_F_NAME'] == exec_f_name)
        &(dg.AccountPermission['AP_L_NAME'] == exec_l_name)
        &(dg.AccountPermission['AP_TAX_ID'] == exec_f_name),
        ['AP_CA_ID','AP_F_NAME','AP_L_NAME','AP_TAX_ID','AP_ACL']]
        size = executioner.shape[0]
        profile['AP_CA_ID']=list(executioner['AP_CA_ID'].values)
        profile['AP_F_NAME']=list(executioner['AP_F_NAME'].values)
        profile['AP_L_NAME']=list(executioner['AP_L_NAME'].values)
        profile['AP_TAX_ID']=list(executioner['AP_TAX_ID'].values)
        profile['AP_ACL']=list(executioner['AP_ACL'].values)
        if size == 0:
            createProfile(profile," trade order no access")
            '''return''' "no access"
     
    if symbol=='' and co_name=='':
        prob = random.uniform(0, 1)
        if prob<0.40:
            company = list(dg.Company['CO_NAME'])
            co_name = company[np.random.randint(len(company))]
            coid = dg.Company.loc[dg.Company['CO_NAME']==co_name,'CO_ID'].values[0]
            issue = list(dg.Security.loc[dg.Security['S_CO_ID']==coid,'S_ISSUE'].values)
            issue = issue[np.random.randint(len(issue))]
        else:
            security = list(dg.Security['S_SYMB'])
            symbol = security[np.random.randint(len(security))]
    if symbol =='':
        company_id = dg.Company.loc[dg.Company['CO_NAME']==co_name,'CO_ID'].values[0]
        profile['CO_NAME'] = [co_name]
        profile['CO_ID'] = [company_id]
        security = dg.Security.loc[(dg.Security['S_CO_ID']==company_id)
        &(dg.Security['S_ISSUE'] == issue),['S_EX_ID','S_NAME','S_SYMB']]
        symbol = security['S_SYMB'].values[0]
        profile['S_CO_ID'] = [company_id]
        profile['S_ISSUE'] = [issue]
        profile['S_EX_ID'] = list(security['S_EX_ID'].values)
        profile['S_NAME'] = list(security['S_NAME'].values)
        profile['S_SYMB'] = list(security['S_SYMB'].values)
    else:
        security = dg.Security.loc[dg.Security['S_SYMB']==symbol,
        ['S_CO_ID','S_EX_ID','S_NAME']]
        profile['S_SYMB'] = [symbol]
        profile['S_CO_ID'] = list(security['S_CO_ID'].values)
        profile['S_EX_ID'] = list(security['S_EX_ID'].values)
        profile['S_NAME'] = list(security['S_NAME'].values)
        company_id = security['S_CO_ID'].values[0]
        co_name = dg.Company.loc[dg.Company['CO_ID']==company_id,'CO_NAME'].values[0]
        profile['CO_ID'] = [company_id]
        profile['CO_NAME'] = [co_name]
    exch_id = security['S_EX_ID'].values[0]
    market_price = dg.LastTrade.loc[dg.LastTrade['LT_S_SYMB']==symbol,'LT_PRICE'].values[0]
    profile['LT_S_SYMB'] = [symbol]
    profile['LT_PRICE'] = [market_price]
   
    if trade_type_id =='':
        prob = random.uniform(0, 1)
        if prob<0.30:
            trade_type_id = 'TMB'
        elif prob<0.60:
            trade_type_id = 'TMS'
        elif prob<0.80:
            trade_type_id = 'TLB'
        elif prob<0.90:
            trade_type_id = 'TLS'
        else:
            trade_type_id = 'TSL'
    tradetype = dg.TradeType.loc[dg.TradeType['TT_ID']==trade_type_id,
    ['TT_IS_MRKT','TT_IS_SELL']]
    type_is_market = tradetype['TT_IS_MRKT'].values[0] 
    type_is_sell = tradetype['TT_IS_SELL'].values[0]
    
    profile['TT_ID'] = [trade_type_id]
    profile['TT_IS_MRKT'] = [type_is_market]
    profile['TT_IS_SELL'] = [type_is_sell]
    
    if requested_price == '':
        requested_price = random.uniform(20,30)
        
    if type_is_market:
        requested_price = market_price
    
    if trade_qty == '' :
        trade_qty = 200*np.random.randint(1,5)
    needed_qty = trade_qty
    buy_value = 0.0
    sell_value = 0.0
    hs_qty = dg.HoldingSummary.loc[(dg.HoldingSummary['HS_CA_ID']==acct_id) & 
    (dg.HoldingSummary['HS_S_SYMB']==symbol),'HS_QTY'].values
    if len(hs_qty) == 0:
        hs_qty = 0
    else:
        hs_qty = hs_qty[0]
    
    profile['H_CA_ID'] = [acct_id]
    profile['H_S_SYMB'] = [symbol]
    profile['H_QTY'] = hs_qty
        
    if is_lifo=='':
        prob = random.uniform(0,1)
        if prob < 0.35:
            is_lifo = 1
        else:
            is_lifo = 0
    
    if type_is_sell:
        if hs_qty>0:
            
            if is_lifo:
                holding = dg.Holding.loc[(dg.Holding['H_CA_ID']==acct_id) & 
                (dg.Holding['H_S_SYMB']==symbol),['H_QTY','H_PRICE','H_DTS']].iloc[::-1]
            else:
                holding = dg.Holding.loc[(dg.Holding['H_CA_ID']==acct_id) & 
                (dg.Holding['H_S_SYMB']==symbol),['H_QTY','H_PRICE','H_DTS']]
            
            for row in holding.iterrows():
                hold_qty = row[1]['H_QTY']
                hold_price = row[1]['H_PRICE']
                if hold_qty > needed_qty:
                    buy_value += needed_qty * hold_price
                    sell_value += needed_qty * requested_price
                    needed_qty = 0
                else :
                    buy_value += hold_qty * hold_price
                    sell_value += hold_qty * requested_price
                    needed_qty = needed_qty - hold_qty
                if needed_qty == 0:
                    break;
                
            profile['H_PRICE'] = list(holding['H_PRICE'].values)
            profile['H_DTS'] = list(holding['H_DTS'].values)
    else:
        if hs_qty<0:
            if is_lifo:
                holding = dg.Holding.loc[(dg.Holding['H_CA_ID']==acct_id) & 
                (dg.Holding['H_S_SYMB']==symbol),['H_QTY','H_PRICE','H_DTS']].iloc[::-1]
            else:
                holding = dg.Holding.loc[(dg.Holding['H_CA_ID']==acct_id) & 
                (dg.Holding['H_S_SYMB']==symbol),['H_QTY','H_PRICE','H_DTS']]
                
            for row in holding.iterrows():
                hold_qty = row[1]['H_QTY']
                hold_price = row[1]['H_PRICE']
                if hold_qty+needed_qty<0:
                    sell_value += needed_qty * hold_price
                    buy_value += needed_qty * requested_price
                    needed_qty = 0
                else :
                    hold_qty = -hold_qty
                    sell_value += hold_qty * hold_price
                    buy_value += hold_qty * requested_price
                    needed_qty = needed_qty - hold_qty
                
                if needed_qty == 0:
                    break;
                    
            profile['H_PRICE'] = list(holding['H_PRICE'].values)
            profile['H_DTS'] = list(holding['H_DTS'].values)
    
    tax_amount = 0.0
    if (sell_value>buy_value) and ((tax_status == 1) or (tax_status == 2)):
        tax_id = dg.CustomerTaxrate.loc[dg.CustomerTaxrate['CX_C_ID']==cust_id,'CX_TX_ID'].values
        tax_id = list(tax_id)
        profile['CX_C_ID'] = [cust_id for i in range(len(tax_id))]
        profile['CX_TX_ID'] = tax_id
        tax_rate = dg.TaxRate.loc[dg.TaxRate['TX_ID'].isin(tax_id),'TX_RATE'].values
        profile['TX_ID'] = [tax_id for i in range(len(tax_rate))]
        profile['TX_RATE'] = list(tax_rate)
        tax_rate = sum(tax_rate)
        tax_amount = (sell_value - buy_value) * tax_rate
    
    comm = dg.CommissionRate.loc[(dg.CommissionRate['CR_C_TIER']==cust_tier)
    &(dg.CommissionRate['CR_TT_ID']==trade_type_id)
    &(dg.CommissionRate['CR_EX_ID']==exch_id)
    &(dg.CommissionRate['CR_FROM_QTY']<=trade_qty)
    &(dg.CommissionRate['CR_TO_QTY']>=trade_qty),['CR_FROM_QTY','CR_TO_QTY','CR_RATE']]
    comm_rate = comm['CR_RATE'].values[0]
    profile['CR_C_TIER'] = [cust_tier]
    profile['CR_TT_ID'] = [trade_type_id]
    profile['CR_EX_ID'] = [exch_id]
    profile['CR_FROM_QTY'] = list(comm['CR_FROM_QTY'].values)
    profile['CR_TO_QTY'] = list(comm['CR_TO_QTY'].values)
    profile['CR_RATE'] = [comm_rate]
    
    charge_amount = dg.Charge.loc[(dg.Charge['CH_C_TIER']==cust_tier)
    &(dg.Charge['CH_TT_ID']==trade_type_id),'CH_CHRG'].values[0]
    profile['CH_C_TIER'] = [cust_tier]
    profile['CH_TT_ID'] = [trade_type_id]
    profile['CH_CHRG'] = [charge_amount]
    
    acct_assets = 0.0
    hold_assets = ''
    if type_is_margin=='':
        prob = random.uniform(0,1)
        if prob<0.08:
            type_is_margin=1
        else:
            type_is_margin=0
    if type_is_margin:
        acct_bal = dg.CustomerAccount.loc[dg.CustomerAccount['CA_ID']==acct_id,'CA_BAL'].values[0]
        profile['CA_BAL'] = [acct_bal]
        holdingsummary = dg.HoldingSummary.loc[dg.HoldingSummary['HS_CA_ID']==acct_id,
        ['HS_CA_ID','HS_S_SYMB','HS_QTY']]
        qty = list(holdingsummary['HS_QTY'].values)
        symb = list(holdingsummary['HS_S_SYMB'].values)
        profile['HS_CA_ID'] = [acct_id for i in range(len(qty))]
        profile['HS_S_SYMB'] = symb
        profile['HS_QTY'] = qty
        for i in range(len(symb)):
            if i==0:
                hold_assets=0
            price = dg.LastTrade.loc[dg.LastTrade['LT_S_SYMB']==symb[i],'LT_PRICE'].values[0]
            profile['LT_S_SYMB'].append(symb[i])
            profile['LT_PRICE'].append(price)
            hold_assets += qty[i]*price
        if hold_assets == '':
            acct_assets = acct_bal
        else:
            acct_assets = hold_assets + acct_bal
    
    if type_is_market:
        status_id = 'SBMT'
    else: 
        status_id = 'PNDG'
    
    comm_amount = (comm_rate / 100) * trade_qty * requested_price
    base_time = max(list(dg.TradeHistory['TH_DTS']))
    base_time = datetime.datetime.strptime(base_time,"%Y-%m-%d %H:%M:%S.%f")
    current_time = datetime.datetime.now()
    current_time = current_time - simulation_start_date + base_time
    current_time_string = datetime.datetime.strftime(current_time,"%Y-%m-%d %H:%M:%S.%f")
    
    trade_id = max(list(dg.Trade['T_ID'])) + 1
    
    if roll_it_back == '':
        prob = random.uniform(0,1)
        roll_it_back = 0
        if prob<0.01:
            roll_it_back = 1
        
    trade = pd.DataFrame({'T_ID': [trade_id] , 'T_DTS': [current_time_string] , 
             'T_ST_ID': [status_id] ,'T_TT_ID': [trade_type_id] , 
             'T_IS_CASH': [abs(1-type_is_margin)] ,'T_S_SYMB': [symbol] , 
             'T_QTY': [trade_qty] , 'T_BID_PRICE': [requested_price] ,
             'T_CA_ID': [acct_id] , 'T_EXEC_NAME': [exec_f_name + ' ' + exec_l_name] , 
             'T_TRADE_PRICE': [0] , 'T_CHRG': [charge_amount] , 'T_COMM': [comm_amount], 
             'T_TAX': [0] , 'T_LIFO': [is_lifo]})
    profile['T_ID'] = list(trade['T_ID'].values)
    profile['T_DTS'] = list(trade['T_DTS'].values)
    profile['T_ST_ID'] = list(trade['T_ST_ID'].values)
    profile['T_TT_ID'] = list(trade['T_TT_ID'].values)
    profile['T_IS_CASH'] = list(trade['T_IS_CASH'].values)
    profile['T_S_SYMB'] = list(trade['T_S_SYMB'].values)
    profile['T_QTY'] = list(trade['T_QTY'].values)
    profile['T_BID_PRICE'] = list(trade['T_BID_PRICE'].values)
    profile['T_CA_ID'] = list(trade['T_CA_ID'].values)
    profile['T_EXEC_NAME'] = list(trade['T_EXEC_NAME'].values)
    profile['T_TRADE_PRICE'] = list(trade['T_TRADE_PRICE'].values)
    profile['T_CHRG'] = list(trade['T_CHRG'].values)
    profile['T_COMM'] = list(trade['T_COMM'].values)
    profile['T_TAX'] = list(trade['T_TAX'].values)
    profile['T_LIFO'] = list(trade['T_LIFO'].values)
    if roll_it_back == 0:
        dg.Trade = dg.Trade.append(trade)
    
    if not type_is_market:
        traderequest_row = pd.DataFrame({'TR_T_ID': [trade_id] , 'TR_TT_ID': [trade_type_id] , 
                            'TR_S_SYMB': [symbol] , 'TR_QTY': [trade_qty] , 
                            'TR_BID_PRICE': [requested_price] , 
                            'TR_B_ID': [broker_id]})
        profile['TR_T_ID'] = list(traderequest_row['TR_T_ID'].values)
        profile['TR_TT_ID'] = list(traderequest_row['TR_TT_ID'].values)
        profile['TR_S_SYMB'] = list(traderequest_row['TR_S_SYMB'].values)
        profile['TR_QTY'] = list(traderequest_row['TR_QTY'].values)
        profile['TR_BID_PRICE'] = list(traderequest_row['TR_BID_PRICE'].values)
        profile['TR_B_ID'] = list(traderequest_row['TR_B_ID'].values)
        if roll_it_back == 0:
            dg.TradeRequest = dg.TradeRequest.append(traderequest_row)
    
    tradehistory_row = pd.DataFrame({'TH_T_ID': [trade_id] , 
                       'TH_DTS': [current_time_string] , 'TH_ST_ID': status_id})
    profile['TH_T_ID'] = list(tradehistory_row['TH_T_ID'].values)
    profile['TH_DTS'] = list(tradehistory_row['TH_DTS'].values)
    profile['TH_ST_ID'] = list(tradehistory_row['TH_ST_ID'].values)
    if roll_it_back == 0:
        dg.TradeHistory = dg.TradeHistory.append(tradehistory_row)
    
    createProfile(profile, " tradeorder " + exec_f_name + ' ' + exec_l_name)
    return acct_id,exec_f_name,exec_l_name,exec_tax_id,is_lifo,co_name,issue,symbol,trade_type_id,trade_qty,type_is_margin,roll_it_back,requested_price,sell_value,buy_value,tax_amount,status_id,trade_id,acct_assets
    
###############################TRADE RESULT TRANSACTION########################
def traderesult(trade_id=''):
    profile = {}
    access_sequence = []
    if trade_id == '':
        trade = list(dg.Trade['T_ID'].values)
        trade_id = trade[np.random.randint(len(trade))]
    trade = dg.Trade.loc[dg.Trade['T_ID']==trade_id,['T_CA_ID','T_TT_ID',
    'T_S_SYMB','T_QTY','T_CHRG','T_LIFO','T_IS_CASH']]
    access_sequence.append('T_ID')
    access_sequence.extend(list(trade.columns))
    profile['T_ID'] = [trade_id]
    profile['T_CA_ID']=list(trade['T_CA_ID'].values)
    profile['T_TT_ID']=list(trade['T_TT_ID'].values)
    profile['T_S_SYMB']=list(trade['T_S_SYMB'].values)
    profile['T_QTY']=list(trade['T_QTY'].values)
    profile['T_CHRG']=list(trade['T_CHRG'].values)
    profile['T_LIFO']=list(trade['T_LIFO'].values)
    profile['T_IS_CASH']=list(trade['T_IS_CASH'].values)
    type_id = trade['T_TT_ID'].values[0]
    symbol = trade['T_S_SYMB'].values[0]
    acct_id = trade['T_CA_ID'].values[0]
    tradetype = dg.TradeType.loc[dg.TradeType['TT_ID']==type_id,['TT_NAME','TT_IS_SELL',
    'TT_IS_MRKT']]
    access_sequence.append('TT_ID')
    access_sequence.extend(list(tradetype.columns))
    profile['TT_ID'] = [type_id]
    profile['TT_NAME'] = list(tradetype['TT_NAME'].values)
    profile['TT_IS_SELL'] = list(tradetype['TT_IS_SELL'].values)
    profile['TT_IS_MRKT'] = list(tradetype['TT_IS_MRKT'].values)
    hs_qty = dg.HoldingSummary.loc[(dg.HoldingSummary['HS_CA_ID']==acct_id) &
    (dg.HoldingSummary['HS_S_SYMB']==symbol),'HS_QTY']
    if hs_qty.shape[0] == 0:
        hs_qty = 0
    else:
        hs_qty = hs_qty.values[0]
    profile['HS_CA_ID'] = [acct_id]
    profile['HS_S_SYMB'] = [symbol]
    profile['HS_QTY'] = [hs_qty]
    access_sequence.append('HS_CA_ID')
    access_sequence.append('HS_S_SYMB')
    access_sequence.append('HS_QTY')
    
    buy_value = 0.0
    sell_value = 0.0
    needed_qty = profile['T_QTY'][0]
    customeraccount = dg.CustomerAccount.loc[dg.CustomerAccount['CA_ID']==acct_id,
    ['CA_B_ID','CA_C_ID','CA_TAX_ST']]
    access_sequence.append('CA_ID')
    access_sequence.extend(list(customeraccount.columns))
    profile['CA_ID'] = [acct_id]
    profile['CA_B_ID'] = list(customeraccount['CA_B_ID'].values)
    broker_id = profile['CA_B_ID'][0]
    profile['CA_C_ID'] = list(customeraccount['CA_C_ID'].values)
    cust_id = profile['CA_C_ID'][0]
    profile['CA_TAX_ST'] = list(customeraccount['CA_TAX_ST'].values)
    type_is_sell = profile['TT_IS_SELL'][0]
    trade_start = min(list(dg.TradeHistory['TH_DTS']))
    trade_start = datetime.datetime.strptime(trade_start,"%Y-%m-%d %H:%M:%S.%f")
    current_time = nowTime()
    trade_dts = datetime.datetime.strftime(current_time,"%Y-%m-%d %H:%M:%S.%f")
    trade_price = marketPrice(symbol, trade_start)
    if type_is_sell:
        if hs_qty == 0:
            holding_row = pd.DataFrame({'HS_CA_ID':[acct_id],'HS_S_SYMB':[symbol],
            'HS_QTY':[-needed_qty]})
            dg.HoldingSummary = dg.HoldingSummary.append(holding_row)
            profile['HS_CA_ID'].append(acct_id)
            profile['HS_S_SYMB'].append(symbol)
            profile['HS_QTY'].append(hs_qty)
            access_sequence.append('HS_CA_ID')
            access_sequence.append('HS_S_SYMB')
            access_sequence.append('HS_QTY')
        elif hs_qty != needed_qty:
            dg.HoldingSummary.loc[(dg.HoldingSummary['HS_CA_ID'] == acct_id) & 
            (dg.HoldingSummary['HS_S_SYMB'] == symbol),'HS_QTY'] = hs_qty - needed_qty
            profile['HS_CA_ID'].append(acct_id)
            profile['HS_S_SYMB'].append(symbol)
            profile['HS_QTY'].append(hs_qty - needed_qty)
            access_sequence.append('HS_CA_ID')
            access_sequence.append('HS_S_SYMB')
            access_sequence.append('HS_QTY')
        is_lifo = profile['T_LIFO'][0]
        
        profile['HH_H_T_ID'] = []
        profile['HH_T_ID'] = []
        profile['HH_BEFORE_QTY'] = []
        profile['HH_AFTER_QTY'] = []
        profile['H_CA_ID'] = []
        profile['H_S_SYMB'] = []
        profile['H_T_ID'] = []
        profile['H_QTY'] = []
        profile['H_PRICE'] = []
        profile['H_DTS'] = []
        if hs_qty>0 :
            if is_lifo:
                holding = dg.Holding.loc[(dg.Holding['H_CA_ID']==acct_id) &
                (dg.Holding['H_S_SYMB']==symbol),['H_T_ID','H_QTY','H_PRICE']].iloc[::-1]
            else:
                 holding = dg.Holding.loc[(dg.Holding['H_CA_ID']==acct_id) &
                 (dg.Holding['H_S_SYMB']==symbol),['H_T_ID','H_QTY','H_PRICE']]
            profile['H_CA_ID'] = [acct_id]
            profile['H_S_SYMB'] = [symbol]
            profile['H_T_ID'] = list(holding['H_T_ID'].values)
            profile['H_QTY'] = list(holding['H_QTY'].values)
            profile['H_PRICE'] = list(holding['H_PRICE'].values)
            access_sequence.append('H_CA_ID')
            access_sequence.append('H_S_SYMB')
            access_sequence.extend(list(holding.columns))
            
            for row in holding.iterrows():
                hold_id = row[1]['H_T_ID']
                hold_qty = row[1]['H_QTY']
                hold_price = row[1]['H_PRICE']
                if hold_qty>needed_qty:
                    holdinghistory = pd.DataFrame({'HH_H_T_ID':[hold_id],
                    'HH_T_ID':[trade_id],'HH_BEFORE_QTY':[hold_qty],
                    'HH_AFTER_QTY':[hold_qty - needed_qty]})
                    dg.HoldingHistory = dg.HoldingHistory.append(holdinghistory)
                    dg.Holding.loc[dg.Holding['H_T_ID']==hold_id,'H_QTY'] = hold_qty - needed_qty
                    profile['HH_H_T_ID'].extend(list(holdinghistory['HH_H_T_ID'].values))
                    profile['HH_T_ID'].extend(list(holdinghistory['HH_T_ID'].values))
                    profile['HH_BEFORE_QTY'].extend(list(holdinghistory['HH_BEFORE_QTY'].values))
                    profile['HH_AFTER_QTY'].extend(list(holdinghistory['HH_AFTER_QTY'].values))
                    profile['H_QTY'].append(hold_qty - needed_qty)
                    access_sequence.extend(list(holdinghistory.columns))
                    access_sequence.append('H_QTY')
                    buy_value += needed_qty * hold_price
                    sell_value += needed_qty * profile['T_QTY'][0]
                    needed_qty = 0
                else:
                    holdinghistory = pd.DataFrame({'HH_H_T_ID':[hold_id],
                    'HH_T_ID':[trade_id],'HH_BEFORE_QTY':[hold_qty],
                    'HH_AFTER_QTY':[0]})
                    dg.HoldingHistory = dg.HoldingHistory.append(holdinghistory)
                    dg.Holding.drop(row[0],inplace = True)
                    profile['HH_H_T_ID'].extend(list(holdinghistory['HH_H_T_ID'].values))
                    profile['HH_T_ID'].extend(list(holdinghistory['HH_T_ID'].values))
                    profile['HH_BEFORE_QTY'].extend(list(holdinghistory['HH_BEFORE_QTY'].values))
                    profile['HH_AFTER_QTY'].extend(list(holdinghistory['HH_AFTER_QTY'].values))
                    access_sequence.extend(list(holdinghistory.columns))
                    buy_value += hold_qty * hold_price
                    sell_value += hold_qty * profile['T_QTY'][0]
                    needed_qty = needed_qty - hold_qty
                if needed_qty == 0:
                    break
            
        if needed_qty>0:
            holdinghistory = pd.DataFrame({'HH_H_T_ID':[trade_id],
            'HH_T_ID':[trade_id],'HH_BEFORE_QTY':[0],'HH_AFTER_QTY':[-1*needed_qty]})
            dg.HoldingHistory = dg.HoldingHistory.append(holdinghistory)
            profile['HH_H_T_ID'].extend(list(holdinghistory['HH_H_T_ID'].values))
            profile['HH_T_ID'].extend(list(holdinghistory['HH_T_ID'].values))
            profile['HH_BEFORE_QTY'].extend(list(holdinghistory['HH_BEFORE_QTY'].values))
            profile['HH_AFTER_QTY'].extend(list(holdinghistory['HH_AFTER_QTY'].values))
            access_sequence.extend(list(holdinghistory.columns))
            holding = pd.DataFrame({'H_T_ID':[trade_id],'H_CA_ID':[acct_id],
            'H_S_SYMB':[symbol],'H_DTS':[trade_dts],'H_PRICE':[trade_price],
            'H_QTY':[-1*needed_qty]})
            dg.Holding = dg.Holding.append(holding)
            profile['H_T_ID'].extend(list(holding['H_T_ID'].values))
            profile['H_CA_ID'].extend(list(holding['H_CA_ID'].values))
            profile['H_S_SYMB'].extend(list(holding['H_S_SYMB'].values))
            profile['H_DTS'].extend(list(holding['H_DTS'].values))
            profile['H_PRICE'].extend(list(holding['H_PRICE'].values))
            profile['H_QTY'].extend(list(holding['H_QTY'].values))
            access_sequence.extend(list(holding.columns))
        else:
            if hs_qty == profile['T_QTY'][0]:
                index = dg.HoldingSummary.loc[(dg.HoldingSummary['HS_CA_ID'] == acct_id)
                &(dg.HoldingSummary['HS_S_SYMB'] == symbol)].index[0]
                dg.HoldingSummary.drop(index,inplace = True)
                profile['HS_CA_ID'].append(acct_id)
                profile['HS_S_SYMB'].append(symbol)
                access_sequence.append(acct_id)
                access_sequence.append(symbol)
    
    else:
        # buy
        if hs_qty == 0:
            holding_row = pd.DataFrame({'HS_CA_ID':[acct_id],'HS_S_SYMB':[symbol],
            'HS_QTY':[-needed_qty]})
            dg.HoldingSummary = dg.HoldingSummary.append(holding_row)
            profile['HS_CA_ID'].append(acct_id)
            profile['HS_S_SYMB'].append(symbol)
            profile['HS_QTY'].append(hs_qty)
            access_sequence.append('HS_CA_ID')
            access_sequence.append('HS_S_SYMB')
            access_sequence.append('HS_QTY')
        elif -1*hs_qty != needed_qty:
            dg.HoldingSummary.loc[(dg.HoldingSummary['HS_CA_ID'] == acct_id) & 
            (dg.HoldingSummary['HS_S_SYMB'] == symbol),'HS_QTY'] = hs_qty + profile['T_QTY'][0]
            profile['HS_CA_ID'].append(acct_id)
            profile['HS_S_SYMB'].append(symbol)
            profile['HS_QTY'].append(hs_qty - needed_qty)
            access_sequence.append('HS_CA_ID')
            access_sequence.append('HS_S_SYMB')
            access_sequence.append('HS_QTY')
        
        is_lifo = profile['T_LIFO'][0]
        
        profile['HH_H_T_ID'] = []
        profile['HH_T_ID'] = []
        profile['HH_BEFORE_QTY'] = []
        profile['HH_AFTER_QTY'] = []
        profile['H_CA_ID'] = []
        profile['H_S_SYMB'] = []
        profile['H_T_ID'] = []
        profile['H_QTY'] = []
        profile['H_PRICE'] = []
        profile['H_DTS'] = []
        
        if hs_qty<0 :
            if is_lifo:
                holding = dg.Holding.loc[(dg.Holding['H_CA_ID']==acct_id) &
                (dg.Holding['H_S_SYMB']==symbol),['H_T_ID','H_QTY','H_PRICE']].iloc[::-1]
            else:
                 holding = dg.Holding.loc[(dg.Holding['H_CA_ID']==acct_id) &
                 (dg.Holding['H_S_SYMB']==symbol),['H_T_ID','H_QTY','H_PRICE']]
            profile['H_CA_ID'] = [acct_id]
            profile['H_S_SYMB'] = [symbol]
            profile['H_T_ID'] = list(holding['H_T_ID'].values)
            profile['H_QTY'] = list(holding['H_QTY'].values)
            profile['H_PRICE'] = list(holding['H_PRICE'].values)
            access_sequence.append('H_CA_ID')
            access_sequence.append('H_S_SYMB')
            access_sequence.extend(list(holding.columns))
            
            for row in holding.iterrows():
                hold_id = row[1]['H_T_ID']
                hold_qty = row[1]['H_QTY']
                hold_price = row[1]['H_PRICE']
                if hold_qty + needed_qty < 0:
                    holdinghistory = pd.DataFrame({'HH_H_T_ID':[hold_id],
                    'HH_T_ID':[trade_id],'HH_BEFORE_QTY':[hold_qty],
                    'HH_AFTER_QTY':[hold_qty + needed_qty]})
                    dg.HoldingHistory = dg.HoldingHistory.append(holdinghistory)
                    dg.Holding.loc[dg.Holding['H_T_ID']==hold_id,'H_QTY'] = hold_qty + needed_qty
                    profile['HH_H_T_ID'].extend(list(holdinghistory['HH_H_T_ID'].values))
                    profile['HH_T_ID'].extend(list(holdinghistory['HH_T_ID'].values))
                    profile['HH_BEFORE_QTY'].extend(list(holdinghistory['HH_BEFORE_QTY'].values))
                    profile['HH_AFTER_QTY'].extend(list(holdinghistory['HH_AFTER_QTY'].values))
                    profile['H_QTY'].append(hold_qty + needed_qty)
                    access_sequence.extend(list(holdinghistory.columns))
                    access_sequence.append('H_QTY')
                    sell_value += needed_qty * hold_price
                    buy_value += needed_qty * profile['T_QTY'][0]
                    needed_qty = 0
                else:
                    holdinghistory = pd.DataFrame({'HH_H_T_ID':[hold_id],
                    'HH_T_ID':[trade_id],'HH_BEFORE_QTY':[hold_qty],
                    'HH_AFTER_QTY':[0]})
                    dg.HoldingHistory = dg.HoldingHistory.append(holdinghistory)
                    dg.Holding.drop(row[0],inplace = True)
                    profile['HH_H_T_ID'].extend(list(holdinghistory['HH_H_T_ID'].values))
                    profile['HH_T_ID'].extend(list(holdinghistory['HH_T_ID'].values))
                    profile['HH_BEFORE_QTY'].extend(list(holdinghistory['HH_BEFORE_QTY'].values))
                    profile['HH_AFTER_QTY'].extend(list(holdinghistory['HH_AFTER_QTY'].values))
                    access_sequence.extend(list(holdinghistory.columns))
                    hold_qty = -hold_qty
                    sell_value += hold_qty * hold_price
                    buy_value += hold_qty * profile['T_QTY'][0]
                    needed_qty = needed_qty - hold_qty
                if needed_qty == 0:
                    break
            
        if needed_qty>0:
            holdinghistory = pd.DataFrame({'HH_H_T_ID':[trade_id],
            'HH_T_ID':[trade_id],'HH_BEFORE_QTY':[0],'HH_AFTER_QTY':[needed_qty]})
            dg.HoldingHistory = dg.HoldingHistory.append(holdinghistory)
            profile['HH_H_T_ID'].extend(list(holdinghistory['HH_H_T_ID'].values))
            profile['HH_T_ID'].extend(list(holdinghistory['HH_T_ID'].values))
            profile['HH_BEFORE_QTY'].extend(list(holdinghistory['HH_BEFORE_QTY'].values))
            profile['HH_AFTER_QTY'].extend(list(holdinghistory['HH_AFTER_QTY'].values))
            access_sequence.extend(list(holdinghistory.columns))
            holding = pd.DataFrame({'H_T_ID':[trade_id],'H_CA_ID':[acct_id],
            'H_S_SYMB':[symbol],'H_DTS':[trade_dts],'H_PRICE':[trade_price],
            'H_QTY':[-1*needed_qty]})
            dg.Holding = dg.Holding.append(holding)
            profile['H_T_ID'].extend(list(holding['H_T_ID'].values))
            profile['H_CA_ID'].extend(list(holding['H_CA_ID'].values))
            profile['H_S_SYMB'].extend(list(holding['H_S_SYMB'].values))
            profile['H_DTS'].extend(list(holding['H_DTS'].values))
            profile['H_PRICE'].extend(list(holding['H_PRICE'].values))
            profile['H_QTY'].extend(list(holding['H_QTY'].values))
            access_sequence.extend(list(holding.columns))
        else:
            if -hs_qty == profile['T_QTY'][0]:
                index = dg.HoldingSummary.loc[(dg.HoldingSummary['HS_CA_ID'] == acct_id)
                &(dg.HoldingSummary['HS_S_SYMB'] == symbol)].index[0]
                dg.HoldingSummary.drop(index,inplace = True)
                profile['HS_CA_ID'].append(acct_id)
                profile['HS_S_SYMB'].append(symbol)
                access_sequence.append(acct_id)
                access_sequence.append(symbol)
        
    tax_amount = 0.0
    if ((profile['CA_TAX_ST'] == 1) or (profile['CA_TAX_ST'] == 2)) and (sell_value>buy_value):
        customertax = list(dg.CustomerTaxrate.loc[dg.CustomerTaxrate['CX_C_ID']==cust_id,
        'CX_TX_ID'].values)
        profile['CX_C_ID'] = [cust_id for i in range(len(customertax))]
        profile['CX_TX_ID'] = [customertax]
        access_sequence.append('CX_C_ID')
        access_sequence.append('CX_TX_ID')
        tax = dg.Taxrate.loc[dg.Taxrate['TX_ID'].isin(customertax),
        ['TX_ID','TX_RATE']]
        profile['TX_ID'] = list(tax['TX_ID'].values)
        profile['TX_RATE'] = list(tax['TX_RATE'].values)
        access_sequence.extend(tax.columns)
        tax_rates = sum(profile['TX_RATE'])
        tax_amount = (sell_value - buy_value) * tax_rates
        dg.Trade.loc[dg.Trade['T_ID'] == trade_id,'T_TAX'] = tax_amount
        profile['T_ID'].append(trade_id)
        profile['T_TAX'] = [tax_amount]
        access_sequence.append('T_ID')
        access_sequence.append('T_TAX')

    security = dg.Security.loc[dg.Security['S_SYMB']==symbol,['S_EX_ID','S_NAME']]
    profile['S_SYMB'] = [symbol]
    profile['S_EX_ID'] = list(security['S_EX_ID'].values)
    profile['S_NAME'] = list(security['S_NAME'].values)
    access_sequence.append('S_SYMB')
    access_sequence.extend(list(security.columns))
    c_tier = dg.Customer.loc[dg.Customer['C_ID']==cust_id,'C_TIER'].values[0]
    profile['C_ID'] = [cust_id]
    profile['C_TIER'] = [c_tier]
    access_sequence.append('C_ID')
    access_sequence.append('C_TIER')
    comm = dg.CommissionRate.loc[(dg.CommissionRate['CR_C_TIER'] == c_tier)
    & (dg.CommissionRate['CR_TT_ID'] == type_id)
    & (dg.CommissionRate['CR_EX_ID'] == profile['S_EX_ID'][0])
    & (dg.CommissionRate['CR_FROM_QTY'] <= profile['T_QTY'][0])
    & (dg.CommissionRate['CR_TO_QTY'] >= profile['T_QTY'][0]),['CR_FROM_QTY','CR_TO_QTY','CR_RATE']]
    profile['CR_C_TIER'] = [c_tier]
    profile['CR_EX_ID'] = profile['S_EX_ID']
    profile['CR_FROM_QTY'] = [comm['CR_FROM_QTY'].values[0]]
    profile['CR_TO_QTY'] = [comm['CR_TO_QTY'].values[0]]
    profile['CR_RATE'] = [comm['CR_RATE'].values[0]]
    comm_rate = profile['CR_RATE'][0]
    access_sequence.append('CR_C_TIER')
    access_sequence.append('CR_TT_ID')
    access_sequence.append('CR_FROM_QTY')
    access_sequence.append('CR_TO_QTY')
    access_sequence.append('CR_RATE')
    
    comm_amount = (comm_rate / 100) * (profile['T_QTY'][0] * trade_price)
    index = dg.Trade['T_ID'] == trade_id
    dg.Trade.loc[index,'T_COMM'] = comm_amount
    dg.Trade.loc[index,'T_DTS'] = trade_dts
    dg.Trade.loc[index,'T_ST_ID'] = 'CMPT' 
    dg.Trade.loc[index,'T_TRADE_PRICE'] = trade_price
    profile['T_ID'].append(trade_id)
    profile['T_COMM'] = comm_amount
    profile['T_DTS'] = trade_dts
    profile['T_ST_ID'] = 'CMPT'
    profile['T_TRADE_PRICE'] = trade_price
    access_sequence.append('T_ID')
    access_sequence.append('T_COMM')
    access_sequence.append('T_DTS')
    access_sequence.append('T_ST_ID')
    access_sequence.append('T_TRADE_PRICE')
    trade = pd.DataFrame({'TH_T_ID': [trade_id],'TH_DTS': [trade_dts],
    'TH_ST_ID': ['CMPT']})
    profile['TH_T_ID'] =  [trade_id]
    profile['TH_DTS'] = [trade_dts]
    profile['TH_ST_ID'] = ['CMPT']
    access_sequence.extend(list(trade.columns))
    dg.TradeHistory = dg.TradeHistory.append(trade)
    index = dg.Broker['B_ID'] == broker_id
    dg.Broker.loc[index,'B_COMM_TOTAL'] += comm_amount
    dg.Broker.loc[index,'B_NUM_TRADES'] += 1
    profile['B_ID'] = [broker_id]
    profile['B_COMM_TOTAL'] = dg.Broker.loc[index,'B_COMM_TOTAL']
    profile['B_NUM_TRADES'] = dg.Broker.loc[index,'B_NUM_TRADES']
    access_sequence.append('B_ID')
    access_sequence.append('B_COMM_TOTAL')
    access_sequence.append('B_NUM_TRADES')
    due_date = current_time + datetime.timedelta(days = 2)
    if type_is_sell :
        se_amount = (profile['T_QTY'][0] * trade_price) - profile['T_CHRG'][0] - comm_amount
    else :
        se_amount = -((profile['T_QTY'][0] * trade_price) + profile['T_CHRG'][0] + comm_amount)
        
    if profile['CA_C_ID'][0] == 1 :
        se_amount = se_amount - tax_amount
        
    if profile['T_IS_CASH'] == 1:
        cash_type = "Cash Account"
    else:
        cash_type = "Margin"
    
    settlement = pd.DataFrame({'SE_T_ID': [trade_id],'SE_CASH_TYPE': [cash_type],
    'SE_CASH_DUE_DATE': [due_date],'SE_AMT': [se_amount]})
    profile['SE_T_ID'] = list(settlement['SE_T_ID'].values)
    profile['SE_CASH_TYPE'] = list(settlement['SE_CASH_TYPE'].values)
    profile['SE_CASH_DUE_DATE'] = list(settlement['SE_CASH_DUE_DATE'].values)
    profile['SE_AMT'] = list(settlement['SE_AMT'].values)
    access_sequence.extend(list(settlement.columns))
    dg.Settlement = dg.Settlement.append(settlement)
    
    profile['CA_BAL'] = []
    if profile['T_IS_CASH'] == 1:
         dg.CustomerAccount.loc[dg.CustomerAccount['CA_ID']==acct_id,'CA_BAL'] += se_amount
         profile['CA_ID'].append(acct_id)
         profile['CA_BAL'] = [dg.CustomerAccount.loc[dg.CustomerAccount['CA_ID']==acct_id,'CA_BAL'].values]
         access_sequence.append('CA_ID')
         access_sequence.append('CA_BAL')
         cashtransaction = pd.DataFrame({'CT_DTS': [trade_dts],'CT_T_ID': [trade_id],
        'CT_AMT': [se_amount],
        'CT_NAME': [profile['TT_NAME'][0] + " " + profile['T_OTY'][0] + " shares of " + profile['S_NAME'][0]]
         })
         profile['CT_DTS'] = list(cashtransaction['CT_DTS'].values)
         profile['CT_T_ID'] = list(cashtransaction['CT_T_ID'].values)
         profile['CT_AMT'] = list(cashtransaction['CT_AMT'].values)
         profile['CT_NAME'] = list(cashtransaction['CT_NAME'].values)
         access_sequence.extend(list(cashtransaction.cloumns))
         dg.CashTransactions = dg.CashTransactions.append(cashtransaction)
    
    acct_bal = dg.CustomerAccount.loc[dg.CustomerAccount['CA_ID']==acct_id,'CA_BAL'].values[0]
    profile['CA_ID'].append(acct_id)
    profile['CA_BAL'].append(acct_bal)
    access_sequence.append('CA_ID')
    access_sequence.append('CA_BAL')
    
    createProfile(profile," tradeResult " + str(trade_id),access_sequence)
    return trade_id,acct_bal,trade_price,acct_id

###############################TRADE STATUS TRANSACTION########################
def tradestatus(acct_id = ''):
    profile = {}
    access_sequence = []
    if acct_id == '':
        group = np.random.randint(2)
        if group == 0:
            customers = list(dg.Customer['C_ID'])
        else:
            tier = np.random.randint(1,4)
            customers = list(dg.Customer.loc[dg.Customer['C_TIER']==tier,'C_ID'])
        cust_id = customers[np.random.randint(len(customers))]
        accounts = list(dg.CustomerAccount.loc[dg.CustomerAccount['CA_C_ID']==cust_id,'CA_ID'])
        acct_id = accounts[np.random.randint(len(accounts))]
    trade = dg.Trade.loc[dg.Trade['T_CA_ID']==acct_id,['T_CA_ID','T_ID','T_DTS',
    'T_S_SYMB','T_QTY','T_EXEC_NAME','T_CHRG','T_ST_ID','T_TT_ID']][-50:]
    access_sequence.extend(list(trade.columns))
    tradetype = dg.TradeType.loc[dg.TradeType['TT_ID'].isin(list(trade['T_TT_ID'].values)),
    ['TT_ID','TT_NAME']]
    access_sequence.extend(list(tradetype.columns))
    statustype = dg.StatusType.loc[dg.StatusType['ST_ID'].isin(list(trade['T_ST_ID'].values)),
    ['ST_ID','ST_NAME']]
    access_sequence.extend(list(statustype.columns))
    security = dg.Security.loc[dg.Security['S_SYMB'].isin(list(trade['T_S_SYMB'].values)),
    ['S_SYMB','S_NAME','S_EX_ID']]
    access_sequence.extend(list(security.columns))
    exchange = dg.Exchange.loc[dg.Exchange['EX_ID'].isin(list(security['S_EX_ID'].values)),
    ['EX_ID','EX_NAME']]
    access_sequence.extend(list(exchange.columns))
    security = security.set_index('S_EX_ID',drop=False).join(exchange.set_index('EX_ID',drop=False))
    trade = trade.set_index('T_TT_ID',drop=False).join(tradetype.set_index('TT_ID',drop=False))
    trade = trade.set_index('T_ST_ID',drop=False).join(statustype.set_index('ST_ID',drop=False))
    trade = trade.set_index('T_S_SYMB',drop=False).join(security.set_index('S_SYMB',drop=False))
    profile['T_CA_ID'] = list(trade['T_CA_ID'].values)
    profile['T_ID'] = list(trade['T_ID'].values)
    profile['T_DTS'] = list(trade['T_DTS'].values)
    profile['T_S_SYMB'] = list(trade['T_S_SYMB'].values)
    profile['T_QTY'] = list(trade['T_QTY'].values)
    profile['T_EXEC_NAME'] = list(trade['T_EXEC_NAME'].values)
    profile['T_CHRG'] = list(trade['T_CHRG'].values)
    profile['T_ST_ID'] = list(trade['T_ST_ID'].values)
    profile['T_TT_ID'] = list(trade['T_TT_ID'].values)
    profile['TT_ID'] = list(trade['TT_ID'].values)
    profile['TT_NAME'] = list(trade['TT_NAME'].values)
    profile['ST_ID'] = list(trade['ST_ID'].values)
    profile['ST_NAME'] = list(trade['ST_NAME'].values)
    profile['S_SYMB'] = list(trade['S_SYMB'].values)
    profile['S_NAME'] = list(trade['S_NAME'].values)
    profile['S_EX_ID'] = list(trade['S_EX_ID'].values)
    profile['EX_ID'] = list(trade['EX_ID'].values)
    profile['EX_NAME'] = list(trade['EX_NAME'].values)
    customeraccount = dg.CustomerAccount.loc[dg.CustomerAccount['CA_ID'] == acct_id,['CA_C_ID','CA_B_ID']]
    customer_id = customeraccount['CA_C_ID'].values[0]
    broker_id = customeraccount['CA_B_ID'].values[0]
    access_sequence.append('CA_ID')
    access_sequence.extend(list(customeraccount.columns))
    customer = dg.Customer.loc[dg.Customer['C_ID']==customer_id,['C_ID',
    'C_F_NAME','C_L_NAME']]
    access_sequence.extend(list(customer.columns))
    broker = dg.Broker.loc[dg.Broker['B_ID']==broker_id,['B_ID','B_NAME']]
    access_sequence.extend(list(broker.columns))
    profile['CA_ID'] = [acct_id]
    profile['CA_C_ID'] = [customer_id]
    profile['CA_B_ID'] = [broker_id]
    profile['C_ID'] = list(customer['C_ID'].values)
    profile['C_F_NAME'] = list(customer['C_F_NAME'].values)
    profile['C_L_NAME'] = list(customer['C_L_NAME'].values)
    profile['B_ID'] = list(broker['B_ID'].values)
    profile['B_NAME'] = list(broker['B_NAME'].values)
    
    createProfile(profile," tradestatus " + str(acct_id))
    return acct_id,trade,customer,broker

##############################TRADE UPDATE TRANSACTION#########################
def tradeupdate(acct_id='',end_trade_dts='',frame_to_execute='',max_acct_id='',max_trades=20,max_updates=20,start_trade_dts='',symbol='',trade_id=''):
    profile = {}
    access_sequence = []
    if frame_to_execute == '':
        prob = random.uniform(0,1);
        if prob<0.33:
            frame_to_execute = 1
        elif prob<0.66:
            frame_to_execute = 2
        else:
            frame_to_execute = 3
    
    if frame_to_execute == 1:
        if trade_id == '':
            trade = list(dg.Trade['T_ID'])
            length = len(trade)
            start = np.random.randint(length-max_trades)
            trade_id = random.sample(trade[start:],max_trades)
        updated = 0
        profile['T_ID'] = []
        profile['T_EXEC_NAME'] = []
        profile['T_BID_PRICE'] = []
        profile['T_IS_CASH'] = []
        profile['T_TRADE_PRICE'] = []
        profile['T_TT_ID'] = []
        profile['TT_ID'] = []
        profile['TT_IS_MRKT'] = []
        profile['SE_T_ID'] = []
        profile['SE_AMT'] = []
        profile['SE_CASH_DUE_DATE'] = []
        profile['SE_CASH_TYPE'] = []
        profile['CT_T_ID'] = []
        profile['CT_AMT'] = []
        profile['CT_DTS'] = []
        profile['CT_NAME'] = []
        profile['TH_T_ID'] = []
        profile['TH_DTS'] = []
        profile['TH_ST_ID'] = []
        for tid in trade_id:
            if updated<max_updates:
                ex_name = dg.Trade.loc[dg.Trade['T_ID']==tid,'T_EXEC_NAME'].values[0]
                profile['T_ID'].append(tid)
                profile['T_EXEC_NAME'].append(ex_name)
                access_sequence.append('T_ID')
                access_sequence.append('T_EXEC_NAME')
                if ex_name.find("X"):
                    ex_name.replace("X"," ")
                else:
                    ex_name.replace(" ","X")
                dg.Trade.loc[dg.Trade['T_ID']==tid,'T_EXEC_NAME'] = ex_name
                profile['T_ID'].append(tid)
                profile['T_EXEC_NAME'].append(ex_name)
                access_sequence.append('T_ID')
                access_sequence.append('T_EXEC_NAME')
                updated += 1
            trade = dg.Trade.loc[dg.Trade['T_ID']==tid,['T_BID_PRICE','T_EXEC_NAME',
            'T_IS_CASH','T_TT_ID','T_TRADE_PRICE']]
            tradetype = dg.TradeType.loc[dg.TradeType['TT_ID'] == trade['T_TT_ID'].values[0],
            ['TT_IS_MRKT']]
            profile['T_ID'].append(tid)
            profile['T_EXEC_NAME'].append(trade['T_EXEC_NAME'].values[0])
            profile['T_BID_PRICE'].append(trade['T_BID_PRICE'].values[0])
            profile['T_IS_CASH'].append(trade['T_IS_CASH'].values[0])
            profile['T_TRADE_PRICE'].append(trade['T_TRADE_PRICE'].values[0])
            profile['T_TT_ID'].append(trade['T_TT_ID'].values[0])
            profile['TT_ID'].append(trade['T_TT_ID'].values[0])
            profile['TT_IS_MRKT'].append(tradetype['TT_IS_MRKT'].values[0])
            access_sequence.append('T_ID')
            access_sequence.extend(trade.columns)
            access_sequence.extend(['TT_ID','TT_IS_MRKT'])
            settlement = dg.Settlement.loc[dg.Settlement['SE_T_ID']==tid,
            ['SE_AMT','SE_CASH_DUE_DATE','SE_CASH_TYPE']]
            profile['SE_T_ID'].append(tid)
            profile['SE_AMT'].append(settlement['SE_AMT'].values[0])
            profile['SE_CASH_DUE_DATE'].append(settlement['SE_CASH_DUE_DATE'].values[0])
            profile['SE_CASH_TYPE'].append(settlement['SE_CASH_TYPE'].values[0])
            access_sequence.append('SE_T_ID')
            access_sequence.extend(settlement.columns)
            if profile['T_IS_CASH'][-1]:
                cashtransaction = dg.CashTransaction.loc[dg.CashTransaction['CT_T_ID'] == tid,
                ['CT_AMT','CT_DTS','CT_NAME']]
                profile['CT_T_ID'].append(tid)
                profile['CT_AMT'].append(cashtransaction['CT_AMT'].values[0])
                profile['CT_DTS'].append(cashtransaction['CT_DTS'].values[0])
                profile['CT_NAME'].append(cashtransaction['CT_NAME'].values[0])
                access_sequence.append('CT_T_ID')
                access_sequence.append(cashtransaction.columns)
            tradehistory = dg.TradeHistory.loc[dg.TradeHistory['TH_T_ID']==tid,
            ['TH_DTS','TH_ST_ID']][:3]
            profile['TH_T_ID'].append(tid)
            profile['TH_DTS'].extend(list(tradehistory['TH_DTS'].values))
            profile['TH_ST_ID'].extend(list(tradehistory['TH_ST_ID'].values))
            access_sequence.append('TH_T_ID')
            access_sequence.extend(tradehistory.columns)
        createProfile(profile, ' tradeupdate1' ,access_sequence)
        return trade_id,frame_to_execute,max_trades,max_updates,profile['T_BID_PRICE'],profile['CT_AMT'],profile['CT_DTS'],profile['CT_NAME'],profile['T_EXEC_NAME'],profile['T_IS_CASH'],profile['TT_IS_MRKT'],profile['SE_AMT'],profile['SE_CASH_DUE_DATE'],profile['SE_CASH_TYPE'],profile['TH_DTS'],profile['TH_ST_ID'],profile['T_TRADE_PRICE']
        
    if frame_to_execute == 2:
        if acct_id == '':
            group = np.random.randint(2)
            if group == 0:
                customers = list(dg.Customer['C_ID'])
            else:
                tier = np.random.randint(1,4)
                customers = list(dg.Customer.loc[dg.Customer['C_TIER']==tier,'C_ID'])
            cust_id = customers[np.random.randint(len(customers))]
            accounts = list(dg.CustomerAccount.loc[dg.CustomerAccount['CA_C_ID']==cust_id,'CA_ID'])
            acct_id = accounts[np.random.randint(len(accounts))]
        if start_trade_dts=='':
            dts = random.sample(list(dg.Trade['T_DTS']),2)
            start_trade_dts = min(dts)
            end_trade_dts = max(dts)
        trade = dg.Trade.loc[(dg.Trade['T_CA_ID']==acct_id)
        &(dg.Trade['T_DTS']>=start_trade_dts)
        &(dg.Trade['T_DTS']<=end_trade_dts),['T_DTS','T_BID_PRICE','T_EXEC_NAME',
        'T_IS_CASH','T_ID','T_TRADE_PRICE']][:max_trades]
        profile['T_CA_ID'] = [acct_id]
        profile['T_DTS'] = list(trade['T_DTS'].values)
        profile['T_BID_PRICE'] = list(trade['T_BID_PRICE'].values)
        profile['T_EXEC_NAME'] = list(trade['T_EXEC_NAME'].values)
        profile['T_IS_CASH'] = list(trade['T_IS_CASH'].values)
        profile['T_ID'] = list(trade['T_ID'].values)
        profile['T_TRADE_PRICE'] = list(trade['T_TRADE_PRICE'].values)
        access_sequence.append('T_CA_ID')
        access_sequence.extend(trade.columns)
        updated = 0
        profile['SE_T_ID'] = []
        profile['SE_AMT'] = []
        profile['SE_CASH_DUE_DATE'] = []
        profile['SE_CASH_TYPE'] = []
        profile['CT_T_ID'] = []
        profile['CT_AMT'] = []
        profile['CT_DTS'] = []
        profile['CT_NAME'] = []
        profile['TH_T_ID'] = []
        profile['TH_DTS'] = []
        profile['TH_ST_ID'] = []
        cnt = 0
        for tid in profile['T_ID']:
            if updated<max_updates:
                cash_type = dg.Settlement.loc[dg.Settlement['SE_T_ID']==tid,'SE_CASH_TYPE'].values[0]
                profile['SE_T_ID'] = [tid]
                profile['SE_CASH_TYPE'] = [cash_type]
                access_sequence.extend(['SE_T_ID','SE_CASH_TYPE'])
                if profile['T_IS_CASH'][cnt]:
                    if cash_type == "Cash Account":
                        cash_type = "Cash"
                    else:
                        cash_type = "Cash Account"
                else:
                    if cash_type == "Margin Account":
                        cash_type = "Margin"
                    else:
                        cash_type = "Margin Account"
                dg.Settlement.loc[dg.Settlement['SE_T_ID']==tid,'SE_CASH_TYPE'].values[0] = cash_type
                profile['SE_T_ID'] = [tid]
                profile['SE_CASH_TYPE'] = [cash_type]
                access_sequence.extend(['SE_T_ID','SE_CASH_TYPE'])
                updated += 1
            settlement = dg.Settlement.loc[dg.Settlement['SE_T_ID']==tid,
            ['SE_AMT','SE_CASH_DUE_DATE','SE_CASH_TYPE']]
            profile['SE_T_ID'].append(tid)
            profile['SE_AMT'].append(settlement['SE_AMT'].values[0])
            profile['SE_CASH_DUE_DATE'].append(settlement['SE_CASH_DUE_DATE'].values[0])
            profile['SE_CASH_TYPE'].append(settlement['SE_CASH_TYPE'].values[0])
            access_sequence.append('SE_T_ID')
            access_sequence.extend(settlement.columns)
            if profile['T_IS_CASH'][cnt]:
                cashtransaction = dg.CashTransaction.loc[dg.CashTransaction['CT_T_ID'] == tid,
                ['CT_AMT','CT_DTS','CT_NAME']]
                profile['CT_T_ID'].append(tid)
                profile['CT_AMT'].append(cashtransaction['CT_AMT'].values[0])
                profile['CT_DTS'].append(cashtransaction['CT_DTS'].values[0])
                profile['CT_NAME'].append(cashtransaction['CT_NAME'].values[0])
                access_sequence.append('CT_T_ID')
                access_sequence.append(cashtransaction.columns)
            tradehistory = dg.TradeHistory.loc[dg.TradeHistory['TH_T_ID']==tid,
            ['TH_DTS','TH_ST_ID']][:3]
            profile['TH_T_ID'].append(tid)
            profile['TH_DTS'].extend(tradehistory['TH_DTS'].values)
            profile['TH_ST_ID'].extend(tradehistory['TH_ST_ID'].values)
            access_sequence.append('TH_T_ID')
            access_sequence.extend(tradehistory.columns)
            cnt += 1
        createProfile(profile, ' tradeupdate2 ' + str(acct_id) ,access_sequence)
        return acct_id,end_trade_dts,start_trade_dts,frame_to_execute,max_trades,max_updates,profile['T_BID_PRICE'],profile['CT_AMT'],profile['CT_DTS'],profile['CT_NAME'],profile['T_EXEC_NAME'],profile['T_IS_CASH'],profile['SE_AMT'],profile['SE_CASH_DUE_DATE'],profile['SE_CASH_TYPE'],profile['TH_DTS'],profile['TH_ST_ID'],profile['T_TRADE_PRICE']
    
    if frame_to_execute == 3:
        if start_trade_dts=='':
            dts = random.sample(list(dg.Trade['T_DTS']),2)
            start_trade_dts = min(dts)
            end_trade_dts = max(dts)
        if symbol == '':
            symbol = random.sample(list(dg.Security['S_SYMB']),1)
            symbol = symbol[0]
        trade = dg.Trade.loc[(dg.Trade['T_S_SYMB'] == symbol)
        &(dg.Trade['T_DTS']>=start_trade_dts)
        &(dg.Trade['T_DTS']<=end_trade_dts),['T_DTS','T_CA_ID','T_EXEC_NAME',
        'T_IS_CASH','T_TRADE_PRICE','T_QTY','T_ID','T_TT_ID']]
        profile['T_S_SYMB'] = [symbol for i in range(trade.shape[0])]
        profile['T_DTS'] = list(trade['T_DTS'].values)
        profile['T_CA_ID'] = list(trade['T_CA_ID'].values)
        profile['T_EXEC_NAME'] = list(trade['T_EXEC_NAME'].values)
        profile['T_IS_CASH'] = list(trade['T_IS_CASH'].values)
        profile['T_TRADE_PRICE'] = list(trade['T_TRADE_PRICE'].values)
        profile['T_QTY'] = list(trade['T_QTY'].values)
        profile['T_ID'] = list(trade['T_ID'].values)
        profile['T_TT_ID'] = list(trade['T_TT_ID'].values)
        profile['TT_ID'] = list(trade['T_TT_ID'].values)
        profile['TT_NAME'] = []
        for tyid in profile['TT_ID']:
            profile['TT_NAME'].append(dg.TradeType.loc[dg.TradeType['TT_ID']==tyid,
                                'TT_NAME'].values[0])
        s_name = dg.Security.loc[dg.Security['S_SYMB']==symbol,'S_NAME'].values[0]
        profile['S_SYMB'] = [symbol for i in range(trade.shape[0])]
        profile['S_NAME'] = [s_name for i in range(trade.shape[0])]
        access_sequence.append('T_S_SYMB')
        access_sequence.extend(trade.columns)
        access_sequence.extend(['TT_ID','TT_NAME','S_SYMB','S_NAME'])
        
        profile['SE_T_ID'] = []
        profile['SE_AMT'] = []
        profile['SE_CASH_DUE_DATE'] = []
        profile['SE_CASH_TYPE'] = []
        profile['CT_T_ID'] = []
        profile['CT_AMT'] = []
        profile['CT_DTS'] = []
        profile['CT_NAME'] = []
        profile['TH_T_ID'] = []
        profile['TH_DTS'] = []
        profile['TH_ST_ID'] = []
        cnt = 0
        updated = 0
        for tid in profile['T_ID']:
            settlement = dg.Settlement.loc[dg.Settlement['SE_T_ID']==tid,
            ['SE_AMT','SE_CASH_DUE_DATE','SE_CASH_TYPE']]
            profile['SE_T_ID'].append(tid)
            profile['SE_AMT'].append(settlement['SE_AMT'].values[0])
            profile['SE_CASH_DUE_DATE'].append(settlement['SE_CASH_DUE_DATE'].values[0])
            profile['SE_CASH_TYPE'].append(settlement['SE_CASH_TYPE'].values[0])
            access_sequence.append('SE_T_ID')
            access_sequence.extend(settlement.columns)
            if profile['T_IS_CASH'][cnt]:
                if updated<max_updates:
                    ct_name = dg.CashTransaction.loc[dg.CashTransaction['CT_T_ID']==tid,'CT_NAME'].values[0]
                    profile['CT_T_ID'].append(tid)
                    profile['CT_NAME'].append(ct_name)
                    access_sequence.extend(['CT_T_ID','CT_NAME'])
                    if ct_name.find("shares of") !=-1:
                        ct_name = profile['TT_NAME'][cnt] + " " + str(profile['T_QTY'][cnt]) + " Shares of " + s_name
                    else:
                        ct_name = profile['TT_NAME'][cnt] + " " + str(profile['T_QTY'][cnt]) + " shares of " + s_name
                    dg.CashTransaction.loc[dg.CashTransaction['CT_T_ID']==tid,'CT_NAME'].values[0] = ct_name
                    profile['CT_T_ID'].append(tid)
                    profile['CT_NAME'].append(ct_name)
                    access_sequence.extend(['CT_T_ID','CT_NAME'])
                    updated += 1
            if profile['T_IS_CASH'][cnt]:
                cashtransaction = dg.CashTransaction.loc[dg.CashTransaction['CT_T_ID'] == tid,
                ['CT_AMT','CT_DTS','CT_NAME']]
                profile['CT_T_ID'].append(tid)
                profile['CT_AMT'].append(cashtransaction['CT_AMT'].values[0])
                profile['CT_DTS'].append(cashtransaction['CT_DTS'].values[0])
                profile['CT_NAME'].append(cashtransaction['CT_NAME'].values[0])
                access_sequence.append('CT_T_ID')
                access_sequence.append(cashtransaction.columns)
            tradehistory = dg.TradeHistory.loc[dg.TradeHistory['TH_T_ID']==tid,
            ['TH_DTS','TH_ST_ID']][:3]
            profile['TH_T_ID'].append(tid)
            profile['TH_DTS'].extend(tradehistory['TH_DTS'].values)
            profile['TH_ST_ID'].extend(tradehistory['TH_ST_ID'].values)
            access_sequence.append('TH_T_ID')
            access_sequence.extend(tradehistory.columns)
            cnt += 1
        createProfile(profile, ' tradeupdate2 ' + str(acct_id) ,access_sequence)
        return symbol,end_trade_dts,start_trade_dts,frame_to_execute,max_trades,max_updates,profile['T_CA_ID'],profile['CT_AMT'],profile['CT_DTS'],profile['CT_NAME'],profile['T_EXEC_NAME'],profile['T_IS_CASH'],profile['T_TRADE_PRICE'],profile['T_QTY'],profile['S_NAME'],profile['SE_AMT'],profile['SE_CASH_DUE_DATE'],profile['SE_CASH_TYPE'],profile['TH_DTS'],profile['TH_ST_ID'],profile['T_ID'],profile['TT_NAME'],profile['T_TT_ID']
            
            
                