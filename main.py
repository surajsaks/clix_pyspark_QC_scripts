import pandas as pd
import os
import time
import pyarrow
import numpy as np
# import findspark
import openpyxl
# findpyspark.init()
import pyspark
import smtplib
from email.message import EmailMessage
import pyspark.pandas as ps
from datetime import date, timedelta
from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext,SparkSession
from pyspark.sql.types import StructType,StructField, StringType, DoubleType,IntegerType, DateType,FloatType,DecimalType
from pyspark.sql.functions import col,last_day,date_format,regexp_replace,when,isnan,explode
import multiprocessing
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import threading
spark = SparkSession.builder.appName('CET').getOrCreate()

def CET_Extract1():
 Customer_Details = pd.read_excel(f'{names}',sheet_name='Customer Details',keep_default_na=False)
 Customer_Details = Customer_Details.astype(str)
 df = pd.read_excel(f'{names}',sheet_name='CAM',keep_default_na=False)
 fin = pd.read_excel(f'{names}',sheet_name='Fin-1',keep_default_na=False)
 ds = pd.read_excel(f'{names}',sheet_name='DSCR - Normal Income + Industry',keep_default_na=False)
 ds1 = pd.read_excel(f'{names}',sheet_name='DSCR - GST Program',keep_default_na=False)
 el = pd.read_excel(f'{names}',sheet_name='Existing Loans',keep_default_na=False,header=None)
 bk = pd.read_excel(f'{names}',sheet_name='Banking',keep_default_na=False)

 # print(el.to_string())
 # df
 # # fin.to_excel("fin.xlsx")
 # cy = pd.to_datetime("now")
 # last_year = cy - pd.Timedelta(365,"day")
 # print(last_year)
 # exit()
 df = df.astype(str)
 AD = pd.DataFrame()
 CM = pd.DataFrame()
 f1 = pd.DataFrame()
 fi = pd.DataFrame()
 df1 = spark.createDataFrame(df)
 df1 = df1.collect()

 CM['risk_Score'] = [df1[11][2]]
 CM['Existing_Exposure'] = [df1[6][2]]
 CM['Total_Exposure'] = [df1[7][2]]
 CM['Max_Eligible_Loan_Amt_per_Risk_Score'] = [df1[12][2]]

 CM['CAM_Average_monthly_Pre_Covid_Business_Credits'] = " "
 CM['CAM_Average_monthly_Post_Covid_Business_Credits'] = " "
 CM['CAM_Post_Covid_V/s_Pre_Covid'] = " "
 CM['CAM_Inward_Bounce'] = " "
 CM['CAM_ABB_EMI'] = " "
 CM['CAM_CC_OD_Utilisation'] = " "
 CM['CAM_Number_of_Bounces'] = " "
 CM["CAM_Outward_Bounce"] = " "

 CM['Deviation_Detail1'] = [df1[78][1]]
 CM['Deviation_Detail2'] = [df1[79][1]]
 CM['Deviation_Detail3'] = [df1[80][1]]
 CM['Deviation_Detail4'] = [df1[81][1]]
 CM['Deviation_Detail5'] = [df1[82][1]]
 CM['Deviation_Detail6'] = [df1[83][1]]
 CM['Deviation_Detail7'] = [df1[84][1]]
 CM['Deviation_Detail8'] = [df1[85][1]]
 CM['Deviation_Detail9'] = [df1[86][1]]
 CM['Deviation_Detail10'] = [df1[87][1]]
 CM['Deviation_Detail11'] = [df1[88][1]]

 CM['Risk_Layer1'] = [df1[78][3]]
 CM['Risk_Layer2'] = [df1[79][3]]
 CM['Risk_Layer3'] = [df1[80][3]]
 CM['Risk_Layer4'] = [df1[81][3]]
 CM['Risk_Layer5'] = [df1[82][3]]
 CM['Risk_Layer6'] = [df1[83][3]]
 CM['Risk_Layer7'] = [df1[84][3]]
 CM['Risk_Layer8'] = [df1[85][3]]
 CM['Risk_Layer9'] = [df1[86][3]]
 CM['Risk_Layer10'] = [df1[87][3]]
 CM['Risk_Layer11'] = [df1[88][3]]

 CM['Mitigant1'] = [df1[78][4]]
 CM['Mitigant2'] = [df1[79][4]]
 CM['Mitigant3'] = [df1[80][4]]
 CM['Mitigant4'] = [df1[81][4]]
 CM['Mitigant5'] = [df1[82][4]]
 CM['Mitigant6'] = [df1[83][4]]
 CM['Mitigant7'] = [df1[84][4]]
 CM['Mitigant8'] = [df1[85][4]]
 CM['Mitigant9'] = [df1[86][4]]
 CM['Mitigant10'] = [df1[87][4]]
 CM['Mitigant11'] = [df1[88][4]]

 CM['Level/Position1'] = [df1[78][2]]
 CM['Level/Position2'] = [df1[79][2]]
 CM['Level/Position3'] = [df1[80][2]]
 CM['Level/Position4'] = [df1[81][2]]
 CM['Level/Position5'] = [df1[82][2]]
 CM['Level/Position6'] = [df1[83][2]]
 CM['Level/Position7'] = [df1[84][2]]
 CM['Level/Position8'] = [df1[85][2]]
 CM['Level/Position9'] = [df1[86][2]]
 CM['Level/Position10'] = [df1[87][2]]
 CM['Level/Position11'] = [df1[88][2]]

 df2 = spark.createDataFrame(Customer_Details)
 df2 = df2.collect()

 AD['applicationId'] = [df2[2][2]]
 AD['Applicant_Detail1'] = [df2[6][0]]
 AD['Applicant_Detail2'] = [df2[7][0]]
 AD['Applicant_Detail3'] = [df2[8][0]]
 AD['Applicant_Detail4'] = [df2[9][0]]
 AD['Applicant_Detail5'] = [df2[10][0]]
 AD['Applicant_Detail6'] = [df2[11][0]]

 AD['Applicant_Name1'] = [df2[6][1]]
 AD['Applicant_Name2'] = [df2[7][1]]
 AD['Applicant_Name3'] = [df2[8][1]]
 AD['Applicant_Name4'] = [df2[9][1]]
 AD['Applicant_Name5'] = [df2[10][1]]
 AD['Applicant_Name6'] = [df2[11][1]]

 AD['Constitution/Relation1'] = [df2[6][2]]
 AD['Constitution/Relation2'] = [df2[7][2]]
 AD['Constitution/Relation3'] = [df2[8][2]]
 AD['Constitution/Relation4'] = [df2[9][2]]
 AD['Constitution/Relation5'] = [df2[10][2]]
 AD['Constitution/Relation6'] = [df2[11][2]]

 AD['Sharholding1'] = [df2[6][3]]
 AD['Sharholding2'] = [df2[7][3]]
 AD['Sharholding3'] = [df2[8][3]]
 AD['Sharholding4'] = [df2[9][3]]
 AD['Sharholding5'] = [df2[10][3]]
 AD['Sharholding6'] = [df2[11][3]]

 AD['cibil_1'] = [df2[6][4]]
 AD['cibil_2'] = [df2[7][4]]
 AD['cibil_3'] = [df2[8][4]]
 AD['cibil_4'] = [df2[8][4]]
 AD['cibil_5'] = [df2[10][4]]
 AD['cibil_6'] = [df2[11][4]]

 AD['DOB1'] = [df2[6][5]]
 AD['DOB2'] = [df2[7][5]]
 AD['DOB3'] = [df2[8][5]]
 AD['DOB4'] = [df2[9][5]]
 AD['DOB5'] = [df2[10][5]]
 AD['DOB6'] = [df2[11][5]]

 AD['Login_date'] = [df2[1][2]]
 AD['Loan_Amount_In_Lacs'] = [df2[1][5]]
 AD['Dsa_Name'] = [df2[4][2]]
 AD['Sm_Name'] = [df2[4][5]]
 AD['Product'] = [df2[2][5]]

 AD['Tendor'] = [df2[2][11]]
 AD['Bamking_Programme'] = [df2[3][5]]
 AD['Branch'] = [df2[3][2]]
 AD['ROI'] = [df2[1][11]]
 AD['EMI'] = [df2[4][11]]
 AD['Industry_Margin'] = [df2[6][12]]

 AD['AGE1'] = [df2[6][6]]
 AD['AGE2'] = [df2[7][6]]
 AD['AGE3'] = [df2[8][6]]
 AD['AGE4'] = [df2[9][6]]
 AD['AGE5'] = [df2[10][6]]
 AD['AGE6'] = [df2[11][6]]

 AD['AGE_at_Maturity1'] = [df2[6][7]]
 AD['AGE_at_Maturity2'] = [df2[7][7]]
 AD['AGE_at_Maturity3'] = [df2[8][7]]
 AD['AGE_at_Maturity4'] = [df2[9][7]]
 AD['AGE_at_Maturity5'] = [df2[10][7]]
 AD['AGE_at_Maturity6'] = [df2[11][7]]

 AD['Business_Type'] = [df2[6][8]]
 AD['Industry'] = [df2[6][9]]
 AD['Segment'] = [df2[6][10]]
 AD['Bus_Product'] = [df2[6][11]]

 AD['Resi_CPV1']  = [df2[6][13]]
 AD['Resi_CPV2']  = [df2[7][13]]
 AD['Resi_CPV3']  = [df2[8][13]]
 AD['Resi_CPV4']  = [df2[9][13]]
 AD['Resi_CPV5']  = [df2[10][13]]
 AD['Resi_CPV6']  = [df2[11][13]]

 AD['Office_CPV1'] = [df2[6][14]]
 AD['Office_CPV2'] = [df2[7][14]]
 AD['Office_CPV3'] = [df2[8][14]]
 AD['Office_CPV4'] = [df2[9][14]]
 AD['Office_CPV5'] = [df2[10][14]]
 AD['Office_CPV6'] = [df2[11][14]]

 AD['Resi_Owned'] = [df2[13][1]]
 AD['Resi_Stability'] = [df2[13][3]]
 AD['Office_Stability'] = [df2[14][3]]
 AD['Office_Owned'] = [df2[14][1]]

 AD['Property_Proof'] = [df2[15][1]]
 AD['Residence_Add'] = [df2[16][1]]
 AD['Office_Add'] = [df2[17][1]]
 AD['Contact_No'] = [df2[18][1]]

 AD['Residence_Add1']=[df2[16][1]]
 AD['Office_Add1'] = [df2[17][1]]
 AD['Contact_No1'] = [df2[18][1]]


 fin = fin.astype(str)
 df3 = spark.createDataFrame(fin)
 df3 = df3.collect()

 f1['Total_Net_Worth1'] = [df3[53][1]]
 f1['Total Net_Worth2'] = [df3[53][2]]
 f1['Total Net_Worth3'] = [df3[53][3]]

 f1['Adjusted_Net_Worth1'] = [df3[55][1]]
 f1['Adjusted_Net_Worth2'] = [df3[55][2]]
 f1['Adjusted_Net_Worth3'] = [df3[55][3]]

 f1['Total_Income1'] = [df3[9][1]]
 f1['Total_Income2'] = [df3[9][2]]
 f1['Total_Income3'] = [df3[9][3]]

 f1['PAT_as_book1'] = [df3[37][1]]
 f1['PAT_as_book2'] = [df3[37][2]]
 f1['PAT_as_book3'] = [df3[37][3]]

 f1['PAT_excl_non_bus1'] = [df3[38][1]]
 f1['PAT_excl_non_bus2'] = [df3[38][2]]
 f1['PAT_excl_non_bus3'] = [df3[38][3]]

 f1['Working_Capital_Limit_from_Banks/FI1'] = [df3[56][1]]
 f1['Working_Capital_Limit_from_Banks/FI2'] = [df3[56][2]]
 f1['Working_Capital_Limit_from_Banks/FI3'] = [df3[56][3]]

 f1['Secure_Loans_Term_Vehicle loans1'] = [df3[57][1]]
 f1['Secure_Loans_Term_Vehicle loans2'] = [df3[57][2]]
 f1['Secure_Loans_Term_Vehicle loans3'] = [df3[57][3]]

 f1['Unsecured_Loans_from_Bank/FI1'] = [df3[58][1]]
 f1['Unsecured_Loans_from_Bank/FI2'] = [df3[58][2]]
 f1['Unsecured_Loans_from_Bank/FI3'] = [df3[58][3]]

 f1['Unsecured_Loans_other1'] = [df3[59][1]]
 f1['Unsecured_Loans_other2'] = [df3[59][2]]
 f1['Unsecured_Loans_other3'] = [df3[59][3]]

 f1['Other_Long_Term_Liabilities1'] = [df3[60][1]]
 f1['Other_Long_Term_Liabilities2'] = [df3[60][2]]
 f1['Other_Long_Term_Liabilities3'] = [df3[60][3]]

 f1['Total_Outside_Borrowing1'] = [df3[61][1]]
 f1['Total_Outside_Borrowing2'] = [df3[61][2]]
 f1['Total_Outside_Borrowing3'] = [df3[61][3]]

 f1['Current_Maturity_of_term_loans1'] = [df3[64][1]]
 f1['Current_Maturity_of_term_loans2'] = [df3[64][2]]
 f1['Current_Maturity_of_term_loans3'] = [df3[64][3]]

 f1['Net_Fixed_Assets1'] = [df3[76][1]]
 f1['Net_Fixed_Assets2'] = [df3[76][2]]
 f1['Net_Fixed_Assets3'] = [df3[76][3]]

 f1['Net_Fixed_Assets_excluding_revaluation1'] = [df3[76][1]]
 f1['Net_Fixed_Assets_excluding_revaluation2'] = [df3[76][2]]
 f1['Net_Fixed_Assets_excluding_revaluation3'] = [df3[76][3]]

 f1['Growth_in_Sales1'] = [df3[98][1]]
 f1['Growth_in_Sales2'] = [df3[98][2]]
 f1['Growth_in_Sales3'] = [df3[98][3]]

 f1['Net_Profit_Margin_Ratio1'] = [df3[99][1]]
 f1['Net_Profit_Margin_Ratio2'] = [df3[99][2]]
 f1['Net_Profit_Margin_Ratio3'] = [df3[99][3]]

 f1['Cash_Profit_Ratio1'] = [df3[100][1]]
 f1['Cash_Profit_Ratio2'] = [df3[100][2]]
 f1['Cash_Profit_Ratio3'] = [df3[100][3]]

 f1['EBIDTA1'] = [df3[101][1]]
 f1['EBIDTA2'] = [df3[101][2]]
 f1['EBIDTA3'] = [df3[101][3]]

 f1['EBIDTA_Ratio1'] = [df3[102][1]]
 f1['EBIDTA_Ratio2'] = [df3[102][2]]
 f1['EBIDTA_Ratio3'] = [df3[102][3]]

 f1['Debt_Equity_Ratio1'] = [df3[103][1]]
 f1['Debt_Equity_Ratio2'] = [df3[103][2]]
 f1['Debt_Equity_Ratio3'] = [df3[103][3]]

 f1['Interest_Coverage_Ratio1'] = [df3[105][1]]
 f1['Interest_Coverage_Ratio2'] = [df3[105][2]]
 f1['Interest_Coverage_Ratio3'] = [df3[105][3]]

 f1['Debtor_Days1'] = [df3[106][1]]
 f1['Debtor_Days2'] = [df3[106][2]]
 f1['Debtor_Days3'] = [df3[106][3]]

 f1['Stock_Days1'] = [df3[107][1]]
 f1['Stock_Days1'] = [df3[107][1]]
 f1['Stock_Days1'] = [df3[107][1]]

 f1['Creditor_Days1'] = [df3[108][1]]
 f1['Creditor_Days2'] = [df3[108][2]]
 f1['Creditor_Days3'] = [df3[108][3]]

 f1['Net_workin_capital_cycles1'] = [df3[109][1]]
 f1['Net_workin_capital_cycles2'] = [df3[109][2]]
 f1['Net_workin_capital_cycles3'] = [df3[109][3]]

 f1['Curr_Ratio_CLincl_OD_CC1'] = [df3[111][1]]
 f1['Curr_Ratio_CLincl_OD_CC2'] = [df3[111][2]]
 f1['Curr_Ratio_CLincl_OD_CC3'] = [df3[111][3]]

 f1['Current_Ratio_CA_Including_Debtors1'] = [df3[112][1]]
 f1['Current_Ratio_CA_Including_Debtors2'] = [df3[112][2]]
 f1['Current_Ratio_CA_Including_Debtors3'] = [df3[112][3]]

 f1['Total_Debt/EBITDA1'] = [df3[113][1]]
 f1['Total_Debt/EBITDA2'] = [df3[113][2]]
 f1['Total_Debt/EBITDA3'] = [df3[113][3]]

 f1['Cost_of_sales1'] = [df3[10][1]]
 f1['Cost_of_sales2'] = [df3[10][2]]
 f1['Cost_of_sales3'] = [df3[10][3]]

 f1['GP_as_per_book1'] =[df3[17][1]]
 f1['GP_as_per_book2'] =[df3[17][2]]
 f1['GP_as_per_book3'] =[df3[17][3]]

 f1['GP_excl_NBI1'] = [df3[18][1]]
 f1['GP_excl_NBI2'] = [df3[18][2]]
 f1['GP_excl_NBI3'] = [df3[18][3]]

 f1['EBITDA_with_Int1'] = [df3[30][1]]
 f1['EBITDA_with_Int2'] = [df3[30][2]]
 f1['EBITDA_with_Int3'] = [df3[30][3]]

 f1['EBITDA_without_Int1'] = [df3[31][1]]
 f1['EBITDA_without_Int2'] = [df3[31][2]]
 f1['EBITDA_without_Int3'] = [df3[31][3]]
 f1['PBT_in_Books']  = [df3[37][3]]


 fi['PBT_Excluding_NBI'] = [df3[38][3]]
 fi['Actual_Cash_Profits'] = [df3[43][3]]
 fi['Cash_Profits'] = [df3[42][3]]

 fi['Current_Liabilities1'] = [df3[68][1]]
 fi['Current_Liabilities2'] = [df3[68][2]]
 fi['Current_Liabilities3'] = [df3[68][3]]

 fi['Liabilities_Outsiders1'] = [df3[67][1]]
 fi['Liabilities_Outsiders2'] = [df3[67][2]]
 fi['Liabilities_Outsiders3'] = [df3[67][3]]

 fi['Investments1'] = [df3[77][1]]
 fi['Investments2'] = [df3[77][2]]
 fi['Investments3'] = [df3[77][3]]

 fi['Current_Assets1'] = [df3[80][1]]
 fi['Current_Assets2'] = [df3[80][2]]
 fi['Current_Assets3'] = [df3[80][3]]

 fi['Loans_and_Advances1'] = [df3[87][1]]
 fi['Loans_and_Advances2'] = [df3[87][2]]
 fi['Loans_and_Advances3'] = [df3[87][3]]

 fi['Total_Assets'] = [df3[94][3]]

 fi['leverage1'] = [df3[104][1]]
 fi['leverage2'] = [df3[104][2]]
 fi['leverage3'] = [df3[104][3]]

 fi['Cash_Genrated_from_Ops'] = [df3[127][3]]
 fi['Net_Cash_used_in_Investing'] = [df3[136][3]]
 fi['Net_Cash_used_in_financing'] = [df3[141][3]]
 fi['Net_increase_in_Cash'] = [df3[142][3]]
 fi['Cash_OB'] = [df3[144][3]]
 fi['Closing_Bal'] = [df3[145][1]]



 ds = ds.astype(str)
 df4 = spark.createDataFrame(ds)
 df4 = df4.collect()

 fi['NCM_Deviation']= [df4[3][5]]

 fi['EBITDA_reported_margin1'] = [df4[11][1]]
 fi['EBITDA_reported_margin2'] = [df4[11][2]]
 fi['EBITDA_reported_margin3'] = [df4[11][3]]

 fi['EBITDA_industry/assessed_margins1'] = [df4[12][1]]
 fi['EBITDA_industry/assessed_margins2'] = [df4[12][2]]
 fi['EBITDA_industry/assessed_margins3'] = [df4[12][3]]

 fi['Existing_Annual_obligations1'] = [df4[31][1]]
 fi['Existing_Annual_obligations2'] = [df4[31][2]]
 fi['Existing_Annual_obligations3'] = [df4[31][3]]

 fi['Proposed_Loan_Obligations1'] = [df4[32][1]]
 fi['Proposed_Loan_Obligations2'] = [df4[32][2]]
 fi['Proposed_Loan_Obligations3'] = [df4[32][3]]

 fi['Total_Obligations1'] = [df4[33][1]]
 fi['Total_Obligations2'] = [df4[33][2]]
 fi['Total_Obligations3'] = [df4[33][3]]

 fi['DSCR_Reported1'] = [df4[37][1]]
 fi['DSCR_Reported2'] = [df4[37][2]]
 fi['DSCR_Reported3'] = [df4[37][3]]

 fi['DSCR_Industry1'] = [df4[38][1]]
 fi['DSCR_Industry2'] = [df4[38][2]]
 fi['DSCR_Industry3'] = [df4[38][3]]

 fi['Firm1_Normal_Gross_Receipts/Turnover1'] = [df4[6][1]]
 fi['Firm1_Normal_Gross_Receipts/Turnover2'] = [df4[6][2]]
 fi['Firm1_Normal_Gross_Receipts/Turnover3'] = [df4[6][3]]

 fi['Firm2_Normal_Gross_Receipts/turnover1'] = [df4[15][1]]
 fi['Firm2_Normal_Gross_Receipts/turnover2'] = [df4[15][2]]
 fi['Firm2_Normal_Gross_Receipts/turnover3'] = [df4[15][3]]


 ds1 = ds1.astype(str)
 df5 = spark.createDataFrame(ds1)
 df5 = df5.collect()

 fi['Firm1_GST_Gross_Receipts/Turnover1'] = [df5[11][2]]
 fi['Firm2_GST_Gross_Receipts/Turnover1'] = [df5[17][2]]
 fi['Gst_TOTAL_EBIDTA_as_per_Industry/Assessed_margins'] = [df5[22][2]]
 fi['GST_DSCR_Basic_GST_margin'] = [df5[32][2]]

 el = el.astype(str)

 df6 = spark.createDataFrame(el)
 df6 = df6.collect()

 fi['Cibil_Enquiry_All_Loan'] = [df6[0][6]]
 fi['Cibil_Enquiry_Unsecured_Loans_Last_3M'] = [df6[1][6]]
 fi['Cash_out_Loan_Last_3M'] = [df6[0][11]]
 fi['EMI_Bounce_Last_3M'] = [df6[1][11]]

 bk = bk.astype(str)

 df7 = spark.createDataFrame(bk)
 df7 = df7.collect()

 fi['Account_Holder_Name1'] = [df7[1][2]]
 fi['Account_Holder_Name2'] = [df7[2][2]]
 fi['Account_Holder_Name3'] = [df7[3][2]]
 fi['Account_Holder_Name4'] = [df7[4][2]]
 fi['Account_Holder_Name5'] = [df7[5][2]]

 fi['Bank_Name1'] = [df7[1][3]]
 fi['Bank_Name2'] = [df7[2][3]]
 fi['Bank_Name3'] = [df7[3][3]]
 fi['Bank_Name4'] = [df7[4][3]]
 fi['Bank_Name5'] = [df7[5][3]]

 fi['Account_No1'] = [df7[1][4]]
 fi['Account_No2'] = [df7[2][4]]
 fi['Account_No3'] = [df7[3][4]]
 fi['Account_No4'] = [df7[4][4]]
 fi['Account_No5'] = [df7[5][4]]

 fi['Account_Type1'] = [df7[1][5]]
 fi['Account_Type2'] = [df7[2][5]]
 fi['Account_Type3'] = [df7[3][5]]
 fi['Account_Type4'] = [df7[4][5]]
 fi['Account_Type5'] = [df7[5][5]]


 bk1 = pd.DataFrame()
 bk1['Sanctioned_Limit1'] = [df7[1][6]]
 bk1['Sanctioned_Limit2'] = [df7[2][6]]
 bk1['Sanctioned_Limit3'] = [df7[3][6]]
 bk1['Sanctioned_Limit4'] = [df7[4][6]]
 bk1['Sanctioned_Limit5'] = [df7[5][6]]

 bk1['Peak_Utilisation1'] = [df7[1][7]]
 bk1['Peak_Utilisation2'] = [df7[2][7]]
 bk1['Peak_Utilisation3'] = [df7[3][7]]
 bk1['Peak_Utilisation4'] = [df7[4][7]]
 bk1['Peak_Utilisation5'] = [df7[5][7]]

 bk1['Average_Monthly_Credits_in_lacs1'] = [df7[1][8]]
 bk1['Average_Monthly_Credits_in_lacs2'] = [df7[2][8]]
 bk1['Average_Monthly_Credits_in_lacs3'] = [df7[3][8]]
 bk1['Average_Monthly_Credits_in_lacs4'] = [df7[4][8]]
 bk1['Average_Monthly_Credits_in_lacs5'] = [df7[5][8]]

 bk1['Avg_Utilisation1'] = [df7[1][9]]
 bk1['Avg_Utilisation2'] = [df7[2][9]]
 bk1['Avg_Utilisation3'] = [df7[3][9]]
 bk1['Avg_Utilisation4'] = [df7[4][9]]
 bk1['Avg_Utilisation5'] = [df7[5][9]]

 bk1['Average_Nos_of_Monthly_Credit_entries1'] = [df7[1][10]]
 bk1['Average_Nos_of_Monthly_Credit_entries2'] = [df7[2][10]]
 bk1['Average_Nos_of_Monthly_Credit_entries3'] = [df7[3][10]]
 bk1['Average_Nos_of_Monthly_Credit_entries4'] = [df7[4][10]]
 bk1['Average_Nos_of_Monthly_Credit_entries5'] = [df7[5][10]]

 bk1['Inward_Bounce1'] = [df7[1][12]]
 bk1['Inward_Bounce2'] = [df7[2][12]]
 bk1['Inward_Bounce3'] = [df7[3][12]]
 bk1['Inward_Bounce4'] = [df7[4][12]]
 bk1['Inward_Bounce5'] = [df7[5][12]]

 bk1['Outward_Bounce1'] = [df7[1][13]]
 bk1['Outward_Bounce2'] = [df7[2][13]]
 bk1['Outward_Bounce3'] = [df7[3][13]]
 bk1['Outward_Bounce4'] = [df7[4][13]]
 bk1['Outward_Bounce5'] = [df7[5][13]]

 bk1['Lowest_ABB_of_last_6M1'] = [df7[1][14]]
 bk1['Lowest_ABB_of_last_6M2'] = [df7[2][14]]
 bk1['Lowest_ABB_of_last_6M3'] = [df7[3][14]]
 bk1['Lowest_ABB_of_last_6M4'] = [df7[4][14]]
 bk1['Lowest_ABB_of_last_6M5'] = [df7[5][14]]

 bk1['Highest_ABB_of_last_6M1'] = [df7[1][15]]
 bk1['Highest_ABB_of_last_6M2'] = [df7[2][15]]
 bk1['Highest_ABB_of_last_6M3'] = [df7[3][15]]
 bk1['Highest_ABB_of_last_6M4'] = [df7[4][15]]
 bk1['Highest_ABB_of_last_6M5'] = [df7[5][15]]

 bk1['Annualised_banking_credits1'] = [df7[1][16]]
 bk1['Annualised_banking_credits2'] = [df7[2][16]]
 bk1['Annualised_banking_credits3'] = [df7[3][16]]
 bk1['Annualised_banking_credits4'] = [df7[4][16]]
 bk1['Annualised_banking_credits5'] = [df7[5][16]]

 bk1['BTO_of_busines_accounts1'] = [df7[1][18]]
 bk1['BTO_of_busines_accounts2'] = [df7[2][18]]
 bk1['BTO_of_busines_accounts3'] = [df7[3][18]]
 bk1['BTO_of_busines_accounts4'] = [df7[4][18]]
 bk1['BTO_of_busines_accounts5'] = [df7[5][18]]

 #Blank_Columns
 bk1['Pre_covid_Average_monthly_credts1'] = " "
 bk1['Pre_covid_Average_monthly_credits2'] = " "
 bk1['Pre_covid_Average_monthly_credits3'] = " "
 bk1['Pre_covid_Average_monthly_credits4'] = " "
 bk1['Pre_covid_Average_monthly_credits5'] = " "
 bk1['Post_covid_Average_monthly_credits1'] = " "
 bk1['Post_covid_Average_monthly_credits2'] = " "
 bk1['Post_covid_Average_monthly_credits3'] = " "
 bk1['Post_covid_Average_monthly_credits4'] = " "
 bk1['Post_covid_Average_monthly_credits5'] = " "

 bk1['Lowest_ABB_EMI_Coverage'] = [df7[13][10]]
 bk1['Latest_3mnths_Prev_3mnths_Credit_Growth'] = [df7[15][10]]
 bk1['Annualised_TO'] = [df7[13][13]]
 bk1['Debit_Credit_No'] = [df7[10][13]]
 bk1['Debit_Credit_Value'] = [df7[11][13]]
 bk1['ABB_EMI_Coverage'] = [df7[15][5]]
 bk1['BTO_Precent_Gst'] = [df7[8][18]]
 bk1['BTO_Precent'] = [df7[7][18]]
 bk1['Avg_CC_OD_Util'] = [df7[8][7]]
 bk1['Inward_Bounce'] = [df7[7][12]]
 bk1['Outward_Bounce'] = [df7[7][13]]
 bk1['EMI_Bounce'] = [df7[8][10]]
 bk1['Post_Covid_BTO'] = " "
 bk1['Post_Covid_Turnover'] = " "

 AD['Index'] = 1
 CM['Index'] = 1
 f1['Index'] = 1
 fi['Index'] = 1
 bk1['Index'] =1
 DC = pd.merge(AD,CM ,on='Index')
 FI = pd.merge(f1,fi ,on='Index')
 FB = pd.merge(FI,bk1 , on='Index')

 final = pd.merge(DC,FB , on='Index')
 final = final.drop('Index',axis=1)

 final.to_csv("/home/pratap103/Cube_file/Asset/abc.csv", mode='a',index=False)

def newdf():
 ab = pd.read_csv('/home/pratap103/Cube_file/Asset/abc.csv')
 ab1 = ab[np.arange(len(ab)) % 2 == 0]
 ab1 = ab1.drop_duplicates(['applicationId'])
 ab1.to_excel("/home/pratap103/Cube_file/Asset/output_files/Combined_CET.xlsx",index=False)
 os.remove('/home/pratap103/Cube_file/Asset/abc.csv')
 #kpi = pd.read_excel('/home/pratap103/Downloads/clix_spark_Cube_Data-main/BL_Cube_Sample_file.xlsx')

def sendemail():
 msg = EmailMessage()
 msg['Subject'] = 'CET File Extraction Completed'
 msg['from'] = 'Sakshath Technologies'
 msg['to'] = 'suraj.sachdeva@sakshath-technologies.com'
 # msg.set_content('Data duplication found on \n'+ f1 + '\n \n \n Lan No. Error found on \n' + f2 +' \n \n \n Null value found DisbDate \n' +f3 )
 msg.set_content("Hey User \n\n"
                 "This is a system generated error mail.\n\n"
                 "CET File Extraction has been succesffuly completed"
                 )
 server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
 server.login('jstmailpratap@gmail.com', "Nov@3064")
 server.send_message(msg)
 server.quit()


if __name__ == '__main__':
    df = spark.read.format('csv').option('delimiter', ' ').load("/home/pratap103/Cube_file/Config/iopath.txt")
    dirpath = df.collect()[1][1]
    outpath = df.collect()[2][1]
    # file_counter = 0
    # inp = '//home//pratap103//Downloads//clix_spark_Cube_Data-main//Jan//North//chandigarh//'
    filelist = []
    for root, dirs, files, in os.walk(dirpath):
        for file in files:
            if file.endswith(".xlsx"):
                filelist.append(os.path.join(root, file))
    # newDF= pd.DataFrame(filelist,index=range(0,len(filelist)))
    # print(newDF)
    t1 = time.perf_counter()
    print('starttime :'+ str(t1)+"\n")
    for names in filelist:
     with concurrent.futures.ProcessPoolExecutor() as executor:
      executor.map(CET_Extract1(),names)
    newdf()
    t2 = time.perf_counter()
    print('Endtime :' + str(t2) + "\n")
    print("Elapsed time during the whole program in seconds:",str(t2 - t1) + "\n \n")
    sendemail()
    # newFinal1 = pd.DataFrame(futures)
    # print(newFinal1.to_string())
    # newFinal1.to_excel(outpath+ "CombinedCET.xlsx", index=False)
    # newFinal = pd.DataFrame(a)
    #newFinal.to_excel(outpath+"abc.xlsx",index=False)



    # for root, dir, files in os.walk(dirpath):
    #     for file in files:
    #         if file.endswith('.xlsx'):
    #             t1 = time.perf_counter()
    #             print('start time:' + str(t1) + "\n")
    #             with concurrent.futures.ProcessPoolExecutor() as executor:
    #                 executor.map(CET_Extract(), file)
    #             print(f"Files Completed {file}\n")
    #             # file_counter += 1
    #             t2 = time.perf_counter()
    #             print('end time :'+str(t2))
    #             print("Elapsed time during the whole program in seconds:",
    #                   str(t2 - t1) + "\n \n")

