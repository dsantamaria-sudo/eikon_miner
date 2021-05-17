#%%
#IMPORT ALL LIBRARIES

import pandas as pd
import numpy as np
import datetime as dt
from datetime import timedelta, date, datetime
import re
from dateutil.relativedelta import relativedelta
import pickle
from itertools import product
import eikon as ek
ek.set_app_key('SET YOUR API KEY HERE')
from pandas._libs.tslibs import Timedelta

#%%
#-------------------------------1º DOWNLOAD SP&500 DAILY HISTORICAL OHLC DATA------------------------------

ric = '.SPX'
end_date = dt.datetime.utcnow()
sp_500 = pd.DataFrame()
start_date = '1999-12-31'

for i in range(3):
    try:
        tmp_df   = ek.get_timeseries(ric, interval = 'daily', start_date = start_date, end_date = end_date)
        end_date = tmp_df.index[0].to_pydatetime()
        tmp_df   = tmp_df.drop(tmp_df.index[0])
        sp_500   = tmp_df.append(sp_500)

        if len(tmp_df) < 2999:
            print('No more tick history available for RIC ' + ric)
            break

    except ek.EikonError as err:

        print(err)
        break

#STORE INFORMATION ON A PICKLE
pickle.dump(df, open("benchmark.p", "wb"))


#-------------------------------2º DOWNLOAD DAILY S&P500 CONSTITUENTS -------------------------------------

#SET SP&500 DATAFRAME INDEX AS DATES MASTER

dates = sp_500.index.tolist()

constituents_by_day = {}

for day in dates:
    try:
        day                      = day.strftime('%Y-%m-%d')
        rics_day                 = ek.get_data(f"0#.SPX({day})", ["TR.RIC"])
        constituents_by_day[day] = list(rics_day[0]['RIC'])
        print(day)
    
    except ek.EikonError as err:
        print(err)
        
#STORE INFORMATION ON A PICKLE
pickle.dump(dict_constituents, open("constituents_by_day.p", "wb"))
         
#-------------------------------3º LIST WITH UNIQUE RICS IN THE WHOLE PERIOD-------------------------------


uniq_rics = []

for i in list(constituents_by_day):
    uniq_rics.append(constituents_by_day[i])

uniq_rics = [item for sublist in uniq_rics for item in sublist]
uniq_rics = list(set(uniq_rics))[1:]

#STORE INFORMATION ON A PICKLE
pickle.dump(uniq_rics, open("uniq_rics.p", "wb"))


#-------------------------------4º DOWNLOAD HISTORICAL OHLC DATA FOR EVERY SP&500 CONSTITUENT--------------

#DOWNLOAD AND STORE HISTORICAL OHLC DATA FOR EVERY SP&500 CONSTITUENT ON THE LIST (uniq_rics).
#ALL THE DATA ARE STORED ON A DICTIONARY(data) WITH RIC AS KEY. INSIDE THE DICTONARY, FOR EACH OF THE RICS 
#A DATAFRAME CAN BE FOUND WITH OHLC AND VOLUME DATA. ALSO POSIBLE ERRORS ARE SAVED.


data ={}
error = []

for ticker in uniq_rics[]:
   
    ric = ticker
    end_date = dt.datetime.utcnow()
    spx = pd.DataFrame()

    for i in range(3):
        try:
            tmp_df = ek.get_timeseries(ric, interval = 'daily', 
            start_date = '1999-12-31', end_date = end_date)
            end_date = tmp_df.index[0].to_pydatetime()
            tmp_df = tmp_df.drop(tmp_df.index[0])
            spx = tmp_df.append(spx)
            if len(tmp_df) < 2999:
                print('No hay mas datos para ' + ric)
                break
        except ek.EikonError as err:
            print(err)
            break
    
    if spx.empty == False:
        print(ticker)    
        data[ticker] = spx
    else: 
        error.append(ticker)

#STORE INFORMATION ON A PICKLE
pickle.dump(data, open("data.p", "wb"))
pickle.dump(error, open("error.p", "wb"))


#-------------------------------5º  OHLC DATA FOR EACH DAY ------------------------------------------------

#ONCE WE HAVE ALL THE DATA WE CAN CREATE A DATAFRAME WITH OHLC DATA FOR EACH DAY WITH ONLY LISTED COMPANIES 
#THAT DAY.AS SOME INCONSISTENCIES ARE FOUND BETWEEN THE LIST OF CONSTITUENTS OFFERED BY EIKON AND THE TIME 
#SERIES DOWNLOADED WE STORE THIS DIFFERENCES AS ERRORS


dates = list(constituents_by_day)
data_by_day = {}
errors = {}

for date in dates: 

    constituents = constituents_by_day[date]

    df = pd.DataFrame(index = constituents , columns = ['HIGH', 'CLOSE', 'LOW', 'OPEN', 'COUNT', 'VOLUME'])

    for i in constituents:

        try:
            a = data[i]
            df.loc[i] = a.loc[date,:]

        except KeyError:

            continue
    
    print(date)

    df = df.dropna(axis=0, how='all' )
    data_by_day[date]= df

    error = list(set(constituents) - set(list(df.index)))
    errors[date]= error

#STORE INFORMATION ON A PICKLE
pickle.dump(data_by_day, open("data_by_day.p", "wb"))
pickle.dump(errores, open("error_by_day.p", "wb"))

#-------------------------------6º SECTOR FOR EACH COMPANY ----------------------------------------------

#WE CAN GET THE SECTOR OF EACH RIC. AS WE HAVE A LIST WITH THE S&P500 CONSTINUENTS 
#SINCE THE YEAR 2000 (STEP 3) WE WILL USE IT.

rics = []
sector = []

for ric in uniq_rics:
    sect = ek.get_data(ric, 'TR.TRBCBusinessSector')[0].iloc[0,1]
    sector.append(sect)
    rics.append(ric)
    print (ric,sect)

data_tuples = list(zip(rics,sector))
ric_sector  = pd.DataFrame(data_tuples, columns=['rics','sector'])

#STORE INFORMATION ON A PICKLE
pickle.dump(df, open("rics_sector.p", "wb"))

#-------------------------------BONUS: VIX DATA ----------------------------------------------------------

end_date = dt.datetime.utcnow()
vix = pd.DataFrame()
ric = '.VIX'
start_date = '1999-12-31'

for i in range(3):
    try:
        tmp_df     = ek.get_timeseries(ric, interval = 'daily', start_date = start_date, end_date = end_date)
        end_date   = tmp_df.index[0].to_pydatetime()
        tmp_df     = tmp_df.drop(tmp_df.index[0])
        vix        = tmp_df.append(spx)
        if len(tmp_df) < 2999:
            print('No more tick history available for RIC ' + ric)
            break
    except ek.EikonError as err:
        print(err)
        break

#STORE INFORMATION ON A PICKLE
pickle.dump(vix, open("rics_sector.p", "wb"))
