

import pandas as pd
import numpy as np
total_columns = []
column_types = []
###################################CUSTOMER####################################

#Account Permission
AccountPermission = pd.read_csv("flat_out/AccountPermission.txt",sep = "|" , header = None)
AccountPermission.columns = ["AP_CA_ID","AP_ACL","AP_TAX_ID","AP_L_NAME","AP_F_NAME"]
#AccountPermission = AccountPermission.set_index(["AP_CA_ID","AP_TAX_ID"],drop = 'false')
col = AccountPermission.dtypes
col_name = list(col.index)
col_type = list(col)
total_columns.extend(col_name)
column_types.extend(col_type)

#Customer
Customer = pd.read_csv("flat_out/Customer.txt",sep = "|" , header = None)
Customer.columns = ["C_ID","C_TAX_ID","C_ST_ID","C_L_NAME","C_F_NAME","C_M_NAME","C_GNDR","C_TIER","C_DOB","C_AD_ID","C_CTRY_1","C_AREA_1","C_LOCAL_1","C_EXT_1","C_CTRY_2","C_AREA_2","C_LOCAL_2","C_EXT_2","C_CTRY_3","C_AREA_3","C_LOCAL_3","C_EXT_3","C_EMAIL_1","C_EMAIL_2"]
#Customer.set_index("C_ID")
col = Customer.dtypes
col_name = list(col.index)
col_type = list(col)
total_columns.extend(col_name)
column_types.extend(col_type)

#Customer Account
CustomerAccount = pd.read_csv("flat_out/CustomerAccount.txt",sep = "|" , header = None)
CustomerAccount.columns = ["CA_ID","CA_B_ID","CA_C_ID","CA_NAME","CA_TAX_ST","CA_BAL"]
#CustomerAccount.set_index("CA_ID")
col = CustomerAccount.dtypes
col_name = list(col.index)
col_type = list(col)
total_columns.extend(col_name)
column_types.extend(col_type)

#Customer Taxrate
CustomerTaxrate = pd.read_csv("flat_out/CustomerTaxrate.txt",sep = "|" , header = None)
CustomerTaxrate.columns = ["CX_TX_ID","CX_C_ID"]
#CustomerTaxrate.set_index(["CX_TX_ID","CX_C_ID"])
col = CustomerTaxrate.dtypes
col_name = list(col.index)
col_type = list(col)
total_columns.extend(col_name)
column_types.extend(col_type)

#Holding
Holding = pd.read_csv("flat_out/Holding.txt",sep = "|" , header = None)
Holding.columns = ["H_T_ID","H_CA_ID","H_S_SYMB","H_DTS","H_PRICE","H_QTY"]
#Holding.set_index("H_T_ID")
col = Holding.dtypes
col_name = list(col.index)
col_type = list(col)
total_columns.extend(col_name)
column_types.extend(col_type)

#Holding History
HoldingHistory = pd.read_csv("flat_out/HoldingHistory.txt",sep = "|" , header = None)
HoldingHistory.columns = ["HH_H_T_ID","HH_T_ID","HH_BEFORE_QTY","HH_AFTER_QTY"]
#HoldingHistory.set_index(["HH_H_T_ID","HH_T_ID"])
col = HoldingHistory.dtypes
col_name = list(col.index)
col_type = list(col)
total_columns.extend(col_name)
column_types.extend(col_type)

#Holding Summary
HoldingSummary = pd.read_csv("flat_out/HoldingSummary.txt",sep = "|" , header = None)
HoldingSummary.columns = ["HS_CA_ID","HS_S_SYMB","HS_QTY"]
#HoldingSummary.set_index(["HS_CA_ID","HS_S_SYMB"])
col = HoldingSummary.dtypes
col_name = list(col.index)
col_type = list(col)
total_columns.extend(col_name)
column_types.extend(col_type)

#Watch Item
WatchItem = pd.read_csv("flat_out/WatchItem.txt",sep = "|" , header = None)
WatchItem.columns = ["WI_WL_ID","WI_S_SYMB"]
#WatchItem.set_index(["WI_WL_ID","WI_S_SYMB"])
col = WatchItem.dtypes
col_name = list(col.index)
col_type = list(col)
total_columns.extend(col_name)
column_types.extend(col_type)

#Watch List
WatchList = pd.read_csv("flat_out/WatchList.txt",sep = "|" , header = None)
WatchList.columns = ["WL_ID","WL_C_ID"]
#WatchList.set_index("WL_ID")
col = WatchList.dtypes
col_name = list(col.index)
col_type = list(col)
total_columns.extend(col_name)
column_types.extend(col_type)
################################BROKER#########################################

#Broker
Broker = pd.read_csv("flat_out/Broker.txt",sep = "|" , header = None)
Broker.columns = ["B_ID","B_ST_ID","B_NAME","B_NUM_TRADES","B_COMM_TOTAL"]
#Broker.set_index("B_ID")
col = Broker.dtypes
col_name = list(col.index)
col_type = list(col)
total_columns.extend(col_name)
column_types.extend(col_type)

#Cash Transactions
CashTransaction = pd.read_csv("flat_out/CashTransaction.txt",sep = "|" , header = None)
CashTransaction.columns = ["CT_T_ID","CT_DTS","CT_AMT","CT_NAME"]
#CashTransaction.set_index("CT_T_ID")
col = CashTransaction.dtypes
col_name = list(col.index)
col_type = list(col)
total_columns.extend(col_name)
column_types.extend(col_type)

#Charge
Charge = pd.read_csv("flat_out/Charge.txt",sep = "|" , header = None)
Charge.columns = ["CH_TT_ID","CH_C_TIER","CH_CHRG"]
#Charge.set_index(["CH_TT_ID","CH_C_TIER"])
col = Charge.dtypes
col_name = list(col.index)
col_type = list(col)
total_columns.extend(col_name)
column_types.extend(col_type)

#Commission Rate
CommissionRate = pd.read_csv("flat_out/CommissionRate.txt",sep = "|" , header = None)
CommissionRate.columns = ["CR_C_TIER","CR_TT_ID","CR_EX_ID","CR_FROM_QTY","CR_TO_QTY","CR_RATE"]
#CommissionRate.set_index(["CR_C_TIER","CR_TT_ID","CR_EX_ID","CR_FROM_QTY"])
col = CommissionRate.dtypes
col_name = list(col.index)
col_type = list(col)
total_columns.extend(col_name)
column_types.extend(col_type)

#Settlement
Settlement = pd.read_csv("flat_out/Settlement.txt",sep = "|" , header = None)
Settlement.columns = ["SE_T_ID","SE_CASH_TYPE","SE_CASH_DUE_DATE","SE_AMT"]
#Settlement.set_index("SE_T_ID")
col = Settlement.dtypes
col_name = list(col.index)
col_type = list(col)
total_columns.extend(col_name)
column_types.extend(col_type)

#Trade
Trade = pd.read_csv("flat_out/Trade.txt",sep = "|" , header = None)
Trade.columns = ["T_ID","T_DTS","T_ST_ID","T_TT_ID","T_IS_CASH","T_S_SYMB","T_QTY","T_BID_PRICE","T_CA_ID","T_EXEC_NAME","T_TRADE_PRICE","T_CHRG","T_COMM","T_TAX","T_LIFO"]
#Trade.set_index("T_ID")
col = Trade.dtypes
col_name = list(col.index)
col_type = list(col)
total_columns.extend(col_name)
column_types.extend(col_type)

#Trade History
TradeHistory = pd.read_csv("flat_out/TradeHistory.txt",sep = "|" , header = None)
TradeHistory.columns = ["TH_T_ID","TH_DTS","TH_ST_ID"]
#TradeHistory.set_index(["TH_T_ID","TH_ST_ID"])
col = TradeHistory.dtypes
col_name = list(col.index)
col_type = list(col)
total_columns.extend(col_name)
column_types.extend(col_type)

#Trade Request
#TradeRequest = pd.read_csv("flat_out/TradeRequest.txt",sep = "|" , header = None)
TradeRequest = pd.DataFrame(columns = ["TR_T_ID","TR_TT_ID","TR_S_SYMB","TR_QTY","TR_BID_PRICE","TR_B_ID"])
#TradeRequest.set_index("TR_T_ID")
col = TradeRequest.dtypes
col_name = list(col.index)
col_type = list(col)
total_columns.extend(col_name)
column_types.extend(col_type)

#Tade Type
TradeType = pd.read_csv("flat_out/TradeType.txt",sep = "|" , header = None)
TradeType.columns = ["TT_ID","TT_NAME","TT_IS_SELL","TT_IS_MRKT"]
#TradeType.set_index("TT_ID")
col = TradeType.dtypes
col_name = list(col.index)
col_type = list(col)
total_columns.extend(col_name)
column_types.extend(col_type)

#########################################MARKET################################

#Company
Company = pd.read_csv("flat_out/Company.txt",sep = "|" , header = None)
Company.columns = ["CO_ID","CO_ST_ID","CO_NAME","CO_IN_ID","CO_SP_RATE","CO_CEO","CO_AD_ID","CO_DESC","CO_OPEN_DATE"]
#Company.set_index("CO_ID")
col = Company.dtypes
col_name = list(col.index)
col_type = list(col)
total_columns.extend(col_name)
column_types.extend(col_type)

#Company Competitor
CompanyCompetitor = pd.read_csv("flat_out/CompanyCompetitor.txt",sep = "|" , header = None)
CompanyCompetitor.columns = ["CP_CO_ID","CP_COMP_CO_ID","CP_IN_ID"]
#CompanyCompetitor.set_index(["CP_CO_ID","CP_COMP_CO_ID","CP_IN_ID"])
col = CompanyCompetitor.dtypes
col_name = list(col.index)
col_type = list(col)
total_columns.extend(col_name)
column_types.extend(col_type)

#Daily Market
DailyMarket = pd.read_csv("flat_out/DailyMarket.txt",sep = "|" , header = None)
DailyMarket.columns = ["DM_DATE","DM_S_SYMB","DM_CLOSE","DM_HIGH","DM_LOW","DM_VOL"]
#DailyMarket.set_index(["DM_DATE","DM_S_SYMB"])
col = DailyMarket.dtypes
col_name = list(col.index)
col_type = list(col)
total_columns.extend(col_name)
column_types.extend(col_type)

#Exchange
Exchange = pd.read_csv("flat_out/Exchange.txt",sep = "|" , header = None)
Exchange.columns = ["EX_ID","EX_NAME","EX_NUM_SYMB","EX_OPEN","EX_CLOSE","EX_DESC","EX_AD_ID"]
#Exchange.set_index("EX_ID")
col = Exchange.dtypes
col_name = list(col.index)
col_type = list(col)
total_columns.extend(col_name)
column_types.extend(col_type)

#Financial
Financial = pd.read_csv("flat_out/Financial.txt",sep = "|" , header = None)
Financial.columns = ["FI_CO_ID","FI_YEAR","FI_QTR","FI_OTR_START_DATE","FI_REVENUE","FI_NET_EARN","FI_BASIC_EPS","FI_DILUT_EPS","FI_MARGIN","FI_INVENTORY","FI_ASSETS","FI_LIABILITY","FI_OUT_BASIC","FI_OUT_DILUT"]
#Financial.set_index(["FI_CO_ID","FI_YEAR","FI_QTR"])
col = Financial.dtypes
col_name = list(col.index)
col_type = list(col)
total_columns.extend(col_name)
column_types.extend(col_type)

#Industry
Industry = pd.read_csv("flat_out/Industry.txt",sep = "|" , header = None)
Industry.columns = ["IN_ID","IN_NAME","IN_SC_ID"]
#Industry.set_index("IN_ID")
col = Industry.dtypes
col_name = list(col.index)
col_type = list(col)
total_columns.extend(col_name)
column_types.extend(col_type)

#Last Trade
LastTrade = pd.read_csv("flat_out/LastTrade.txt",sep = "|" , header = None)
LastTrade.columns = ["LT_S_SYMB","LT_DTS","LT_PRICE","LT_OPEN_PRICE","LT_VOL"]
#LastTrade.set_index("LT_S_SYMB")
col = LastTrade.dtypes
col_name = list(col.index)
col_type = list(col)
total_columns.extend(col_name)
column_types.extend(col_type)

#News Item
NewsItem = pd.read_csv("flat_out/NewsItem.txt",sep = "|" , header = None)
NewsItem.columns = ["NI_ID","NI_HEADLINE","NI_SUMMARY","NI_ITEM","NI_DTS","NI_SOURCE","NI_AUTHOR"]
#NewsItem.set_index("NI_ID")
col = NewsItem.dtypes
col_name = list(col.index)
col_type = list(col)
total_columns.extend(col_name)
column_types.extend(col_type)

#News Xref
NewsXRef = pd.read_csv("flat_out/NewsXRef.txt",sep = "|" , header = None)
NewsXRef.columns = ["NX_NI_ID", "NX_CO_ID"]
#NewsXRef.set_index(["NX_NI_ID", "NX_CO_ID"])
col = NewsXRef.dtypes
col_name = list(col.index)
col_type = list(col)
total_columns.extend(col_name)
column_types.extend(col_type)

#Sector
Sector = pd.read_csv("flat_out/Sector.txt",sep = "|" , header = None)
Sector.columns = ["SC_ID", "SC_NAME"]
#Sector.set_index("SC_ID")
col = Sector.dtypes
col_name = list(col.index)
col_type = list(col)
total_columns.extend(col_name)
column_types.extend(col_type)

#Security
Security = pd.read_csv("flat_out/Security.txt",sep = "|" , header = None)
Security.columns = ["S_SYMB", "S_ISSUE","S_ST_ID","S_NAME","S_EX_ID","S_CO_ID","S_NUM_OUT","S_START_DATE","S_EXCH_DATE","S_PE","S_52WK_HIGH","S_52WK_HIGH_DATE","S_52WK_LOW","S_52WK_LOW_DATE","S_DIVIDEND","S_YIELD"]
#Security.set_index("S_SYMB")
col = Security.dtypes
col_name = list(col.index)
col_type = list(col)
total_columns.extend(col_name)
column_types.extend(col_type)

##############################DIMENSION########################################

#Address
Address = pd.read_csv("flat_out/Address.txt",sep = "|" , header = None)
Address.columns = ["AD_ID", "AD_LINE1","AD_LINE2","AD_ZC_CODE","AD_CTRY"]
#Address.set_index("AD_ID")
col = Address.dtypes
col_name = list(col.index)
col_type = list(col)
total_columns.extend(col_name)
column_types.extend(col_type)

#Status Type
StatusType = pd.read_csv("flat_out/StatusType.txt",sep = "|" , header = None)
StatusType.columns = ["ST_ID", "ST_NAME"]
#StatusType.set_index("ST_ID")
col = StatusType.dtypes
col_name = list(col.index)
col_type = list(col)
total_columns.extend(col_name)
column_types.extend(col_type)

#Tax Rate
TaxRate = pd.read_csv("flat_out/TaxRate.txt",sep = "|" , header = None)
TaxRate.columns = ["TX_ID", "TX_NAME","TX_RATE"]
#TaxRate.set_index("TX_ID")
col = TaxRate.dtypes
col_name = list(col.index)
col_type = list(col)
total_columns.extend(col_name)
column_types.extend(col_type)

#ZIP Code
ZipCode = pd.read_csv("flat_out/ZipCode.txt",sep = "|" , header = None)
ZipCode.columns = ["ZC_CODE", "ZC_TOWN","ZC_DIV"]
#ZipCode.set_index("ZC_CODE")
col = ZipCode.dtypes
col_name = list(col.index)
col_type = list(col)
total_columns.extend(col_name)
column_types.extend(col_type)

#URV Template Construction
URV_feature_type =  {}
for i in range(len(total_columns)):
    if total_columns[i].find('_ID') !=-1:
        URV_feature_type[total_columns[i]] = 'C' 
    elif column_types[i] == np.float64 or column_types[i] == np.int64:
        URV_feature_type[total_columns[i]] = 'N' 
    else:
        URV_feature_type[total_columns[i]] = 'C'
        
exception = ['AP_ACL','C_AREA_1','C_AREA_2','C_AREA_3','C_CTRY_1','C_CTRY_2','C_CTRY_3',
             'C_EXT_1','C_EXT_2','C_EXT_3','C_LOCAL_1','C_LOCAL_2','C_LOCAL_3',
             'C_TIER', 'CA_TAX_ST','CH_CHRG','CH_C_TIER','CR_C_TIER','T_IS_CASH',
             'T_LIFO','TR_QTY','TR_BID_PRICE','TT_IS_MRKT','TT_IS_SELL','FI_QTR']

for e in exception:
    if URV_feature_type[e] == 'C':
        URV_feature_type[e] = 'N'
    else:
        URV_feature_type[e] = 'C'
        
URV_feature_index = {}
start = 0
for i in total_columns:
    URV_feature_index[i] = start
    if URV_feature_type[i] == 'C':
        start = start + 2
    else:
        start = start + 5

URV_size = start
    
        
'''foreign_keys = ['AP_CA_ID','C_ST_ID','C_AD_ID','CA_B_ID','CA_C_ID','CX_TX_ID','CX_C_ID','H_T_ID','H_CA_ID','H_S_SYMB','HH_H_T_ID',
                'HH_T_ID','HS_CA_ID','HS_S_SYMB','WI_WL_ID','WI_S_SYMB','WL_C_ID','B_ST_ID','CT_T_ID','CH_TT_ID','CR_TT_ID','CR_EX_ID',
                'SE_T_ID','T_ST_ID','T_TT_ID','T_S_SYMB','T_CA_ID','TH_T_ID','TH_ST_ID','TR_T_ID','TR_TT_ID','TR_S_SYMB','TR_B_ID','CO_ST_ID',
                'CO_IN_ID','CO_AD_ID','CP_CO_ID','CP_COMP_CO_ID','CP_IN_ID','DM_S_SYMB','EX_AD_ID','FI_CO_ID','IN_SC_ID','LT_S_SYMB',
                'NX_NI_ID','NX_CO_ID','S_ST_ID','S_EX_ID','S_CO_ID','AD_ZC_CODE']'''


    
'''for fk in foreign_keys:
    pos = fk.find('_')
    feature = fk[pos+1:]
    try:
        feature_mapping[fk] = feature_mapping[feature]
    except:
        pos = feature.find('_')
        feature = feature[pos+1:]
        feature_mapping[fk] = feature_mapping[feature]
        
    
URV_base_features = list(set(total_columns)-set(foreign_keys))

#True for numerical and false for categorical including ordinal 
#ID's are of type int64 so initially all int64 are false
def Repeat(x): 
    _size = len(x) 
    repeated = [] 
    for i in range(_size): 
        k = i + 1
        for j in range(k, _size): 
            if x[i] == x[j] and x[i] not in repeated: 
                repeated.append(x[i]) 
    return repeated '''