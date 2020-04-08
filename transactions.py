#creating different transaction on the database created in flat_out folder
#transactions are defined in the TPC-E specification pdf. 

import datagen as dg
import numpy as np
import pandas as pd
import random
import datetime
simulation_start_date = datetime.datetime.now()

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
    trade_start = min(list(dg.TradeHistory['TH_DTS']))
    base_time = max(list(dg.TradeHistory['TH_DTS']))
    base_time = datetime.datetime.strptime(base_time,"%Y-%m-%d %H:%M:%S.%f")
    current_time = datetime.datetime.now()
    current_time = current_time - simulation_start_date + base_time
    security = random.sample(list(dg.Security['S_SYMB']),20)
 
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
        access_lob_flag = np.random.randint(2)
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
        frame_to_execute=np.random.randint(1,5)

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
        
###############################TRADE STATUS TRANSACTION########################
def tradestatus(acct_id = ''):
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
    trade = dg.Trade.loc[dg.Trade['T_CA_ID']==acct_id,['T_CA_ID','T_ID','T_DTS',
    'T_S_SYMB','T_QTY','T_EXEC_NAME','T_CHRG','T_ST_ID','T_TT_ID']][-50:]
    tradetype = dg.TradeType.loc[dg.TradeType['TT_ID'].isin(list(trade['T_TT_ID'].values)),
    ['TT_ID','TT_NAME']]
    statustype = dg.StatusType.loc[dg.StatusType['ST_ID'].isin(list(trade['T_ST_ID'].values)),
    ['ST_ID','ST_NAME']]
    security = dg.Security.loc[dg.Security['S_SYMB'].isin(list(trade['T_S_SYMB'].values)),
    ['S_SYMB','S_NAME','S_EX_ID']]
    exchange = dg.Exchange.loc[dg.Exchange['EX_ID'].isin(list(security['S_EX_ID'].values)),
    ['EX_ID','EX_NAME']]
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
    customer = dg.Customer.loc[dg.Customer['C_ID']==customer_id,['C_ID',
    'C_F_NAME','C_L_NAME']]
    broker = dg.Broker.loc[dg.Broker['B_ID']==broker_id,['B_ID','B_NAME']]
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