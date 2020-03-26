

import pandas as pd

###################################CUSTOMER####################################

#Account Permission
AccountPermission = pd.read_csv("flat_out/AccountPermission.txt",sep = "|" , header = None)
AccountPermission.columns = ["AP_CA_ID","AP_ACL","AP_TAX_ID","AP_L_NAME","AP_F_NAME"]
#AccountPermission = AccountPermission.set_index(["AP_CA_ID","AP_TAX_ID"],drop = 'false')

#Customer
Customer = pd.read_csv("flat_out/Customer.txt",sep = "|" , header = None)
Customer.columns = ["C_ID","C_TAX_ID","C_ST_ID","C_L_NAME","C_F_NAME","C_M_NAME","C_GNDR","C_TIER","C_DOB","C_AD_ID","C_CTRY_1","C_AREA_1","C_LOCAL_1","C_EXT_1","C_CTRY_2","C_AREA_2","C_LOCAL_2","C_EXT_2","C_CTRY_3","C_AREA_3","C_LOCAL_3","C_EXT_3","C_EMAIL_1","C_EMAIL_2"]
#Customer.set_index("C_ID")

#Customer Account
CustomerAccount = pd.read_csv("flat_out/CustomerAccount.txt",sep = "|" , header = None)
CustomerAccount.columns = ["CA_ID","CA_B_ID","CA_C_ID","CA_NAME","CA_TAX_ST","CA_BAL"]
#CustomerAccount.set_index("CA_ID")

#Customer Taxrate
CustomerTaxrate = pd.read_csv("flat_out/CustomerTaxrate.txt",sep = "|" , header = None)
CustomerTaxrate.columns = ["CX_TX_ID","CX_C_ID"]
#CustomerTaxrate.set_index(["CX_TX_ID","CX_C_ID"])

#Holding
Holding = pd.read_csv("flat_out/Holding.txt",sep = "|" , header = None)
Holding.columns = ["H_T_ID","H_CA_ID","H_S_SYMB","H_DTS","H_PRICE","H_QTY"]
#Holding.set_index("H_T_ID")

#Holding History
HoldingHistory = pd.read_csv("flat_out/HoldingHistory.txt",sep = "|" , header = None)
HoldingHistory.columns = ["HH_H_T_ID","HH_T_ID","HH_BEFORE_QTY","H_AFTER_QTY"]
#HoldingHistory.set_index(["HH_H_T_ID","HH_T_ID"])

#Holding Summary
HoldingSummary = pd.read_csv("flat_out/HoldingSummary.txt",sep = "|" , header = None)
HoldingSummary.columns = ["HS_CA_ID","HS_S_SYMB","HS_QTY"]
#HoldingSummary.set_index(["HS_CA_ID","HS_S_SYMB"])

#Watch Item
WatchItem = pd.read_csv("flat_out/WatchItem.txt",sep = "|" , header = None)
WatchItem.columns = ["WI_WL_ID","WI_S_SYMB"]
#WatchItem.set_index(["WI_WL_ID","WI_S_SYMB"])

#Watch List
WatchList = pd.read_csv("flat_out/WatchList.txt",sep = "|" , header = None)
WatchList.columns = ["WL_ID","WL_C_ID"]
#WatchList.set_index("WL_ID")

################################BROKER#########################################

#Broker
Broker = pd.read_csv("flat_out/Broker.txt",sep = "|" , header = None)
Broker.columns = ["B_ID","B_ST_ID","B_NAME","B_NUM_TRADES","B_COMM_TOTAL"]
#Broker.set_index("B_ID")

#Cash Transactions
CashTransaction = pd.read_csv("flat_out/CashTransaction.txt",sep = "|" , header = None)
CashTransaction.columns = ["CT_T_ID","CT_DTS","CT_AMT","CT_NAME"]
#CashTransaction.set_index("CT_T_ID")

#Charge
Charge = pd.read_csv("flat_out/Charge.txt",sep = "|" , header = None)
Charge.columns = ["CH_TT_ID","CH_C_TIER","CH_CHRG"]
#Charge.set_index(["CH_TT_ID","CH_C_TIER"])

#Commission Rate
CommissionRate = pd.read_csv("flat_out/CommissionRate.txt",sep = "|" , header = None)
CommissionRate.columns = ["CR_C_TIER","CR_TT_ID","CR_EX_ID","CR_FROM_QTY","CR_TO_QTY","CR_RATE"]
#CommissionRate.set_index(["CR_C_TIER","CR_TT_ID","CR_EX_ID","CR_FROM_QTY"])

#Settlement
Settlement = pd.read_csv("flat_out/Settlement.txt",sep = "|" , header = None)
Settlement.columns = ["SE_T_ID","SE_CASH_TYPE","SE_CASH_DUE_DATE","SE_AMT"]
#Settlement.set_index("SE_T_ID")

#Trade
Trade = pd.read_csv("flat_out/Trade.txt",sep = "|" , header = None)
Trade.columns = ["T_ID","T_DTS","T_ST_ID","T_TT_ID","T_IS_CASH","T_S_SYMB","T_QTY","T_BID_PRICE","T_CA_ID","T_EXEC_NAME","T_TRADE_PRICE","T_CHRG","T_COMM","T_TAX","T_LIFO"]
#Trade.set_index("T_ID")

#Trade History
TradeHistory = pd.read_csv("flat_out/TradeHistory.txt",sep = "|" , header = None)
TradeHistory.columns = ["TH_T_ID","TH_DTS","TH_ST_ID"]
TradeHistory.set_index(["TH_T_ID","TH_ST_ID"])

#Trade Request
#TradeRequest = pd.read_csv("flat_out/TradeRequest.txt",sep = "|" , header = None)
TradeRequest = pd.DataFrame(columns = ["TR_T_ID","TR_TT_ID","TR_S_SYMB","TR_QTY","TR_BID_PRICE","TR_B_ID"])
TradeRequest.set_index("TR_T_ID")

#Tade Type
TradeType = pd.read_csv("flat_out/TradeType.txt",sep = "|" , header = None)
TradeType.columns = ["TT_ID","TT_NAME","TT_IS_SELL","TT_IS_MRKT"]
#TradeType.set_index("TT_ID")

#########################################MARKET################################

#Company
Company = pd.read_csv("flat_out/Company.txt",sep = "|" , header = None)
Company.columns = ["CO_ID","CO_ST_ID","CO_NAME","CO_IN_ID","CO_SP_RATE","CO_CEO","CO_AD_ID","CO_DESC","CO_OPEN_DATE"]
#Company.set_index("CO_ID")

#Company Competitor
CompanyCompetitor = pd.read_csv("flat_out/CompanyCompetitor.txt",sep = "|" , header = None)
CompanyCompetitor.columns = ["CP_CO_ID","CP_COMP_CO_ID","CP_IN_ID"]
#CompanyCompetitor.set_index(["CP_CO_ID","CP_COMP_CO_ID","CP_IN_ID"])

#Daily Market
DailyMarket = pd.read_csv("flat_out/DailyMarket.txt",sep = "|" , header = None)
DailyMarket.columns = ["DM_DATE","DM_S_SYMB","DM_CLOSE","DM_HIGH","DM_LOW","DM_VOL"]
#DailyMarket.set_index(["DM_DATE","DM_S_SYMB"])

#Exchange
Exchange = pd.read_csv("flat_out/Exchange.txt",sep = "|" , header = None)
Exchange.columns = ["EX_ID","EX_NAME","EX_NUM_SYMB","EX_OPEN","EX_CLOSE","EX_DESC","EX_AD_ID"]
#Exchange.set_index("EX_ID")

#Financial
Financial = pd.read_csv("flat_out/Financial.txt",sep = "|" , header = None)
Financial.columns = ["FI_CO_ID","FI_YEAR","FI_QTR","FI_OTR_START_DATE","FI_REVENUE","FI_NET_EARN","FI_BASIC_EPS","FI_DILUT_EPS","FI_MARGIN","FI_INVENTORY","FI_ASSETS","FI_LIABILITY","FI_OUT_BASIC","FI_OUT_DILUT"]
#Financial.set_index(["FI_CO_ID","FI_YEAR","FI_QTR"])

#Industry
Industry = pd.read_csv("flat_out/Industry.txt",sep = "|" , header = None)
Industry.columns = ["IN_ID","IN_NAME","IN_SC_ID"]
#Industry.set_index("IN_ID")

#Last Trade
LastTrade = pd.read_csv("flat_out/LastTrade.txt",sep = "|" , header = None)
LastTrade.columns = ["LT_S_SYMB","LT_DTS","LT_PRICE","LT_OPEN_PRICE","LT_VOL"]
#LastTrade.set_index("LT_S_SYMB")

#News Item
NewsItem = pd.read_csv("flat_out/NewsItem.txt",sep = "|" , header = None)
NewsItem.columns = ["NI_ID","NI_HEADLINE","NI_SUMMARY","NI_ITEM","NI_DTS","NI_SOURCE","NI_AUTHOR"]
#NewsItem.set_index("NI_ID")

#News Xref
NewsXRef = pd.read_csv("flat_out/NewsXRef.txt",sep = "|" , header = None)
NewsXRef.columns = ["NX_NI_ID", "NX_CO_ID"]
#NewsXRef.set_index(["NX_NI_ID", "NX_CO_ID"])

#Sector
Sector = pd.read_csv("flat_out/Sector.txt",sep = "|" , header = None)
Sector.columns = ["SC_ID", "SC_NAME"]
#Sector.set_index("SC_ID")

#Security
Security = pd.read_csv("flat_out/Security.txt",sep = "|" , header = None)
Security.columns = ["S_SYMB", "S_ISSUE","S_ST_ID","S_NAME","S_EX_ID","S_CO_ID","S_NUM_OUT","S_START_DATE","S_EXCH_DATE","S_PE","S_52WK_HIGH","S_52WK_HIGH_DATE","S_52WK_LOW","S_52WK_LOW_DATE","S_DIVIDEND","S_YIELD"]
#Security.set_index("S_SYMB")

##############################DIMENSION########################################

#Address
Address = pd.read_csv("flat_out/Address.txt",sep = "|" , header = None)
Address.columns = ["AD_ID", "AD_LINE1","AD_LINE2","AD_ZC_CODE","AD_CTRY"]
#Address.set_index("AD_ID")

#Status Type
StatusType = pd.read_csv("flat_out/StatusType.txt",sep = "|" , header = None)
StatusType.columns = ["ST_ID", "ST_NAME"]
#StatusType.set_index("ST_ID")

#Tax Rate
TaxRate = pd.read_csv("flat_out/TaxRate.txt",sep = "|" , header = None)
TaxRate.columns = ["TX_ID", "TX_NAME","TX_RATE"]
#TaxRate.set_index("TX_ID")

#ZIP Code
ZipCode = pd.read_csv("flat_out/ZipCode.txt",sep = "|" , header = None)
ZipCode.columns = ["ZC_CODE", "ZC_TOWN","ZC_DIV"]
#ZipCode.set_index("ZC_CODE")