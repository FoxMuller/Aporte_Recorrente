#!/usr/bin/env python
# coding: utf-8

# V0.11 - 2022-08-12<br> 
# - fii_dividends = Adicionado o filtro fii para pegar dados do Status Invest

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import datetime as dt
from get_DividendsSI import get_DividendsSI

def yf_dividends(df, timeframe):
    
    div = df[df['Dividends']>0]
    agg_dict = {
                      'Open': 'first',
                      'High': 'max',
                      'Low': 'min',
                      'Close': 'last',
                      'Dividends': 'sum',
                      'Stock Splits': 'sum',
                      'Volume': 'sum'}
    
    if (timeframe=="monthly" or timeframe=="1mo"):
        
        df['Day']=df.index
        df['Day']=df['Day'].dt.day
        df.index = np.where( (df['Day']>26) & (df['Dividends']==0) ,
                                    pd.to_datetime(df.index)+ np.timedelta64(+1, 'D'),
                                    df.index)
        df = df.drop(columns=['Day'])
       
        df = df.resample('M').agg(agg_dict)
        df['Date_Index']=df.index
        df["Date"]=pd.to_datetime(df['Date_Index'].dt.year.astype(str)+"-"+df['Date_Index'].dt.month.astype(str)+"-01")
        df.set_index("Date", inplace=True)
        df.drop('Date_Index', axis=1, inplace=True)
        df.dropna(axis = 0, inplace=True)
    elif (timeframe=="weekly" or timeframe=="1wk"):
        df['DateNew']=df.index
        df['WeekDay']=df['DateNew'].dt.weekday
        df['DateNew']=np.where(df['WeekDay']==6,df['DateNew']+timedelta(1),df['DateNew'])
        df.set_index('DateNew', drop=True, inplace=True)
        df.index.names = ['Date']
        df = df.resample('W').agg(agg_dict)
        df.index=df.index-timedelta(6)     #Transforma em Weekly
        df.dropna(axis = 0, inplace=True)
    else:
        df.dropna(axis = 0, inplace=True)
        
    return df

def fii_dividends(fii, df, interval, is_fii=True):
    fii=fii.upper()
    if fii[-3:]==".SA":
        fii=fii.replace(".SA","")
        
    df.drop(['Dividends'], axis = 1, inplace=True)
    div = get_DividendsSI(fii, is_fii)
    
    df = pd.merge(df,div, how='outer', on='Date', validate='one_to_many', sort=True)
    df['Dividends']=df['Dividends'].fillna(0)
    df['Stock Splits']=0
    
    div = df[df['Dividends']>0]
    agg_dict = {
                      'Open': 'first',
                      'High': 'max',
                      'Low': 'min',
                      'Close': 'last',
                      'Dividends': 'sum',
                      'Stock Splits': 'sum',
                      'Volume': 'sum'}
    
    
    if (interval=="monthly" or interval=="1mo"):
        
        df['Day']=df.index
        df['Day']=df['Day'].dt.day
        df.index = np.where( (df['Day']>26) & (df['Dividends']==0) ,
                                    pd.to_datetime(df.index)+ np.timedelta64(+1, 'D'),
                                    df.index)
        df = df.drop(columns=['Day'])
       
        df = df.resample('M').agg(agg_dict)
        df['Date_Index']=df.index
        df["Date"]=pd.to_datetime(df['Date_Index'].dt.year.astype(str)+"-"+df['Date_Index'].dt.month.astype(str)+"-01")
        df.set_index("Date", inplace=True)
        df.drop('Date_Index', axis=1, inplace=True)
        df.dropna(axis = 0, inplace=True)
    elif (interval=="weekly" or interval=="1wk"):
        df['DateNew']=df.index
        df['WeekDay']=df['DateNew'].dt.weekday
        df['DateNew']=np.where(df['WeekDay']==6,df['DateNew']+timedelta(1),df['DateNew'])
        df.set_index('DateNew', drop=True, inplace=True)
        df.index.names = ['Date']
        df = df.resample('W').agg(agg_dict)
        df.index=df.index-timedelta(6)     #Transforma em Weekly
        df.dropna(axis = 0, inplace=True)
    else:
        df.dropna(axis = 0, inplace=True)
        
    return df
