#!/usr/bin/env python
# coding: utf-8

# <h2><b>Aporte Mensal</b></h2>
# <h4>Autor: Felipe Muller - cmtemuller@gmail.com</h4>
# <h4>Versao: 0.81 - 2022-07-22</h4>

# v0.82 - CORRIGIR: O cdi do resto ser tb baseado no rf_cdi
# 
# v0.81 - 2022-07-23<br>
# - Better use of return_type summary and row
# - Summary, informações do Filtro utilizado
# 
# V0.80 - 2022-07-22<br> 
# - Funcionalidade 'weekday'
# - Renamed 'weekday' and 'week-weekday' to 'daily-weekday' and 'daily-week-weekend'
# - Minor Corrections
# 
# v0.78 - 2022-07-22<br>
# - Adicionadi Filtro "Sem sinal"
# 
# v0.77 - 2022-07-20<br>
# - Adicioando no summary a Linha: Custos
# 
# v0.76 - 2022-07-17<br> 
# - Compra no dia da semana e semana do mês<br>
# 
# V0.75 - 2022-07-15<br> 
# - Colocado o Stock Split no cálculo
# - atualizado o Custos (havia um erro em que ele sempre calculva todos os meses
# - Diário Funcionando
# 
# v0.74 - 2022-07-14<br> 
# - Count Reverse
# 
# v0.73 - 2022-07-12<br> 
# - Semanal e Mensal funcionando
# 
# v0.72 - 2022-07-02<br> 
# - Dia do pregão mensal a ser feito o aporte
# 
# V0.71 - 2022-06-21<br> 
# - Otimizado o uso do filtro_sinal

# 1. Bibliotecas<br>

import numpy as np
import pandas as pd
import yfinance as yf
from datetime import datetime, date
import ta

import sys
if not sys.warnoptions:
    import warnings
    warnings.simplefilter("ignore")
    
from get_debit import get_debit
from real_br_money_mask import real_br_money_mask
from yf_dividends import yf_dividends

# 2. Funções<br>

def AporteRecorrente_getdata(ticker, startdate, enddate, timeframe):
    
    if (timeframe=="monthly"):
        delta=24
        interval = '1mo'
        new_stardate = pd.to_datetime(startdate) - np.timedelta64(delta+1, 'M')
    
    elif (timeframe=="weekly"):
        delta=110
        interval = '1wk'
        new_stardate = pd.to_datetime(startdate) - np.timedelta64(delta+1, 'W')
    
    else: #Aqui fica incluso o daily-week-weekday e daily-weekday
        delta=700
        interval = '1d'
        new_stardate = pd.to_datetime(startdate) - np.timedelta64(delta+1, 'D')
    
    dados = yf.Ticker(ticker).history(start=new_stardate,end=enddate,interval=interval,auto_adjust = False)
    dados = yf_dividends(dados, timeframe)
    
    ipca=get_debit("ipca")
    cdi=get_debit("cdi")

    df = dados.dropna(axis = 0)
    
    df['Date_New']=df.index
    df['Day']=df['Date_New'].dt.day
    df['Day']=pd.to_numeric(df['Day'])
    
    if (timeframe=="monthly"):
        df.index = np.where( df['Day']>1,
                            pd.to_datetime(df.index)+ np.timedelta64(+1, 'D'),
                            df.index)
    else:
        df.index = pd.to_datetime(df.index)
    
    df.index.name='Date'
    
    df.drop(columns=['High', 'Low', 'Volume', 'Date_New', 'Day'], axis=1, inplace=True)
    
    #Criação da quantidade de Count_Month
    df['Date_New']=df.index
    
    df['Day']=df['Date_New'].dt.day
    df['Month']=df['Date_New'].dt.month
    df['Year']=df['Date_New'].dt.year
    df['WeekDay']=df['Date_New'].dt.weekday+1
                
    df['Count_Month']=1
    df['Count_Month_Rev']=-1
    df['Count_Week']=1
                
    df['ipca']=0.
    df['cdi']=0.
    
    df=df.reset_index()
    for i in range(df.shape[0]):
        if i>0:
            if timeframe=="daily-week-weekday" or timeframe=='daily-weekday':
                if df['Month'][i]!=df['Month'][i-1]:
                    if df['WeekDay'][i]>df['WeekDay'][i-1]:
                        df['Month'][i]=df['Month'][i-1]
                        df['Count_Week'][i]=df['Count_Week'][i-1]
                    else:
                        df['Count_Week'][i]=1
                else:
                    if df['WeekDay'][i]>df['WeekDay'][i-1]:
                        df['Count_Week'][i]=df['Count_Week'][i-1]
                    else:
                        df['Count_Week'][i]=df['Count_Week'][i-1]+1
            else:
                if df['Month'][i]==df['Month'][i-1]:
                    df['Count_Month'][i]=df['Count_Month'][i-1]+1
        try:
            df['ipca'][i]=ipca.loc[pd.to_datetime(str(df['Year'][i])+"/"+str(df['Month'][i])+"/"+str(df['Count_Month'][i]))]['ipca']
            df['cdi'][i]=cdi.loc[pd.to_datetime(str(df['Year'][i])+"/"+str(df['Month'][i])+"/"+str(df['Count_Month'][i]))]['cdi']
        except:
            df['ipca'][i]=df['ipca'][i-1]
            df['cdi'][i]=df['cdi'][i-1]
    
    if timeframe=='daily-week-weekday' or timeframe=='daily-weekday':
        df['Count_Month']=df['Count_Week']*10+df['WeekDay']
        
    #df.dropna(axis=0, inplace=True)
    
    df.set_index('Date', inplace=True)
    
    #Reverse Count_Month_Rev
    rev = df.resample('M').agg({'Count_Month': 'max'})
    rev['Date']=rev.index
    rev['Month']=rev['Date'].dt.month
    rev['Year']=rev['Date'].dt.year
    rev.set_index(['Year','Month'],inplace=True)
    rev=rev.drop('Date', axis = 1)
    
    df=df.reset_index()
    for i in range(df.shape[0]):
        df['Count_Month_Rev'][i]=df['Count_Month'][i]-rev.loc[df['Year'][i],df['Month'][i]]['Count_Month']-1
    
    if timeframe=='daily-week-weekday':
        df['Count_Month_Rev']=df['Count_Month_Rev']
        
    df.set_index('Date', inplace=True)
    if timeframe == 'daily-weekday':
        df.drop(columns=['Date_New', 'Day', 'Month','Year','Count_Week'], inplace=True)
    else:
        df.drop(columns=['Date_New', 'Day', 'Month','Year','Count_Week','WeekDay'], inplace=True)
    return df

def AporteRecorrente (money ,  df, timeframe, startdate, enddate, rf_cdi 
                 , corretagem, issqn, bovespa 
                 , filtro , filtro_periodos , filtro_rsi , filtro_asc, filtro_sinal
                 , pregao
                 , return_type ):
    
    if timeframe=="monthly":
        pregao = 1 
        
    if (filtro.lower()=="sma"):
        df['Filtro_value']=round(ta.trend.SMAIndicator(df['Close'], window = filtro_periodos, fillna = False).sma_indicator(),2)
        df['Filtro']=0
    elif (filtro.lower()=="rsi"):
        df['Filtro_value']=round(ta.momentum.RSIIndicator(df['Close'], window = filtro_periodos, fillna = True).rsi(),2)
        df['Filtro']=0
    else:
        df['Filtro']=1
        df['Filtro_value']=0
    
    df=df[startdate:]
    df.dropna(axis=0, inplace=True)
    df=df.reset_index()
    
    df['money_acum']=float(money)
    df['money_equiv']=float(money)
    df['rf_acum']=float(money)
    df['ipca_acum']=float(df['ipca'][0])
    df['cdi_acum']=float(df['cdi'][0]*(rf_cdi/100))
    df['custos']=float(0.)
    df['resto']=float(0.)
    df['stock']=int(0)
    df['stock_acum']=int(0)
    
    
    for i in range(df.shape[0]):
        if i==0:
            if timeframe=='daily-weekday':
                df['resto'][i]=money
                
            if filtro_sinal == 'none':
                df['Filtro'][i]=1
            else:
                if filtro.lower()=="sma":
                    if filtro_sinal=="<=":
                        if df['Close'][i]<=df['Filtro_value'][i]:
                            df['Filtro'][i]=1
                    elif filtro_sinal=="<":
                        if df['Close'][i]<df['Filtro_value'][i]:
                            df['Filtro'][i]=1
                    elif filtro_sinal==">":
                        if df['Close'][i]>df['Filtro_value'][i]:
                            df['Filtro'][i]=1
                    else: 
                        if df['Close'][i]>=df['Filtro_value'][i]:
                            df['Filtro'][i]=1

                elif filtro.lower()=="rsi":
                    if filtro_sinal=="<=":
                        if df['Filtro_value'][i]<=filtro_rsi:
                            df['Filtro'][i]=1
                    elif filtro_sinal=="<":
                        if df['Filtro_value'][i]<filtro_rsi:
                            df['Filtro'][i]=1
                    elif filtro_sinal==">":
                        if df['Filtro_value'][i]>filtro_rsi:
                            df['Filtro'][i]=1
                    else:
                        if df['Filtro_value'][i]>=filtro_rsi:
                            df['Filtro'][i]=1
                    
        else: #if i==0:
            # START - definicao do IPCA, CDI, money_equiv, renda fixa e money_acum            
            if timeframe=="daily-week-weekday":
                if df['Count_Month'][i]<df['Count_Month'][i-1]:
                    df['ipca_acum'][i]=(1+df['ipca'][i])*(1+df['ipca_acum'][i-1])-1
                    df['cdi_acum'][i]=(1+df['cdi'][i])*(1+df['cdi_acum'][i-1]*(rf_cdi/100))-1
                    df['money_equiv'][i]=round(money*(1+df['ipca_acum'][i-1]),2)
                    df['rf_acum'][i]=round(df['money_equiv'][i]+df['rf_acum'][i-1]*(1+df['cdi'][i]),2)
                    df['money_acum'][i]=round(df['money_acum'][i-1]+df['money_equiv'][i],2)
                else:     
                    df['ipca_acum'][i]=df['ipca_acum'][i-1]
                    df['cdi_acum'][i]=df['cdi_acum'][i-1]
                    df['money_equiv'][i]=df['money_equiv'][i-1]
                    df['rf_acum'][i]=df['rf_acum'][i-1]
                    df['money_acum'][i]=df['money_acum'][i-1]
            elif timeframe=="daily-weekday":
                if df['Count_Month'][i]<df['Count_Month'][i-1]:
                    df['ipca_acum'][i]=(1+df['ipca'][i])*(1+df['ipca_acum'][i-1])-1
                    df['cdi_acum'][i]=(1+df['cdi'][i])*(1+df['cdi_acum'][i-1]*(rf_cdi/100))-1
                    df['money_equiv'][i]=round(money*(1+df['ipca_acum'][i-1]),2)
                    if df['WeekDay'][i]<df['WeekDay'][i-1]:        
                        df['rf_acum'][i]=round(df['money_equiv'][i]+df['rf_acum'][i-1]*(1+df['cdi'][i]),2)
                        df['money_acum'][i]=round(df['money_acum'][i-1]+df['money_equiv'][i],2)
                    else:
                        df['rf_acum'][i]=df['rf_acum'][i-1]
                        df['money_acum'][i]=df['money_acum'][i-1]
                else:
                    df['ipca_acum'][i]=df['ipca_acum'][i-1]
                    df['cdi_acum'][i]=df['cdi_acum'][i-1]
                    df['money_equiv'][i]=df['money_equiv'][i-1]
                    if df['WeekDay'][i]<df['WeekDay'][i-1]:        
                        df['rf_acum'][i]=round(df['money_equiv'][i]+df['rf_acum'][i-1],2)
                        df['money_acum'][i]=round(df['money_equiv'][i]+df['money_acum'][i-1],2)
                    else:
                        df['rf_acum'][i]=df['rf_acum'][i-1]
                        df['money_acum'][i]=df['money_acum'][i-1]
                
            else: #if timeframe=="daily-week-weekday" or "daily-weekday"
                if pregao == 0:
                    if df['Count_Month'][i]==1:
                        df['ipca_acum'][i]=(1+df['ipca'][i])*(1+df['ipca_acum'][i-1])-1
                        df['cdi_acum'][i]=(1+df['cdi'][i])*(1+df['cdi_acum'][i-1]*(rf_cdi/100))-1
                        df['money_equiv'][i]=round(money*(1+df['ipca_acum'][i-1]),2)
                        df['rf_acum'][i]=round(df['money_equiv'][i]+df['rf_acum'][i-1]*(1+df['cdi'][i]),2)
                        df['money_acum'][i]=round(df['money_acum'][i-1]+df['money_equiv'][i],2)
                    else:
                        df['ipca_acum'][i]=df['ipca_acum'][i-1]
                        df['cdi_acum'][i]=df['cdi_acum'][i-1]
                        df['money_equiv'][i]=df['money_equiv'][i-1]
                        df['rf_acum'][i]=df['money_equiv'][i]+df['rf_acum'][i-1]
                        df['money_acum'][i]=round(df['money_acum'][i-1]+df['money_equiv'][i],2)
               
                else:
                    if df['Count_Month'][i]==1:
                        df['ipca_acum'][i]=(1+df['ipca'][i])*(1+df['ipca_acum'][i-1])-1
                        df['cdi_acum'][i]=(1+df['cdi'][i])*(1+df['cdi_acum'][i-1]*(rf_cdi/100))-1
                        df['money_equiv'][i]=round(money*(1+df['ipca_acum'][i-1]),2)
                        df['rf_acum'][i]=round(df['money_equiv'][i]+df['rf_acum'][i-1]*(1+df['cdi'][i]),2)
                        df['money_acum'][i]=round(df['money_acum'][i-1]+df['money_equiv'][i],2)
                    else:
                        df['ipca_acum'][i]=df['ipca_acum'][i-1]
                        df['cdi_acum'][i]=df['cdi_acum'][i-1]
                        df['money_equiv'][i]=df['money_equiv'][i-1]
                        df['rf_acum'][i]=df['rf_acum'][i-1]
                        df['money_acum'][i]=df['money_acum'][i-1]
            # END - definicao do IPCA, CDI, money_equiv, renda fixa e money_acum            
            if filtro.lower()=="sma":
                if filtro_sinal=="<=":
                    if filtro_asc==True:
                        if (df['Close'][i]<=df['Filtro_value'][i] and df['Filtro_value'][i]>=df['Filtro_value'][i-1]):
                            df['Filtro'][i]=1
                    else:
                        if df['Close'][i]<=df['Filtro_value'][i]:
                            df['Filtro'][i]=1
                elif filtro_sinal=="<":
                    if filtro_asc==True:
                        if (df['Close'][i]<df['Filtro_value'][i] and df['Filtro_value'][i]>=df['Filtro_value'][i-1]):
                            df['Filtro'][i]=1
                    else:
                        if df['Close'][i]<df['Filtro_value'][i]:
                            df['Filtro'][i]=1
                elif filtro_sinal==">":
                    if filtro_asc==True:
                        if (df['Close'][i]>df['Filtro_value'][i] and df['Filtro_value'][i]>=df['Filtro_value'][i-1]):
                            df['Filtro'][i]=1
                    else:
                        if df['Close'][i]>df['Filtro_value'][i]:
                            df['Filtro'][i]=1
                elif filtro_sinal=='none':
                    if filtro_asc==True:
                        if (df['Filtro_value'][i]>=df['Filtro_value'][i-1]):
                            df['Filtro'][i]=1
                    else:
                        df['Filtro'][i]=0
                else:
                    if filtro_asc==True:
                        if (df['Close'][i]>=df['Filtro_value'][i] and df['Filtro_value'][i]>=df['Filtro_value'][i-1]):
                            df['Filtro'][i]=1
                    else:
                        if df['Close'][i]>=df['Filtro_value'][i]:
                            df['Filtro'][i]=1
                            
            elif filtro.lower()=="rsi":
                if filtro_sinal=="<=":
                    if filtro_asc==True:
                        if (df['Filtro_value'][i]<=filtro_rsi and df['Filtro_value'][i]>=df['Filtro_value'][i-1]):
                            df['Filtro'][i]=1
                    else:
                        if df['Filtro_value'][i]<=filtro_rsi:
                            df['Filtro'][i]=1
                elif filtro_sinal=="<":
                    if filtro_asc==True:
                        if (df['Filtro_value'][i]<filtro_rsi and df['Filtro_value'][i]>=df['Filtro_value'][i-1]):
                            df['Filtro'][i]=1
                    else:
                        if df['Filtro_value'][i]<filtro_rsi:
                            df['Filtro'][i]=1
                elif filtro_sinal==">":
                    if filtro_asc==True:
                        if (df['Filtro_value'][i]>filtro_rsi and df['Filtro_value'][i]>=df['Filtro_value'][i-1]):
                            df['Filtro'][i]=1
                    else:
                        if df['Filtro_value'][i]>filtro_rsi:
                            df['Filtro'][i]=1
                elif filtro_sinal=='none':
                    if filtro_asc==True:
                        if (df['Filtro_value'][i]>=df['Filtro_value'][i-1]):
                            df['Filtro'][i]=1
                    else:
                        df['Filtro'][i]=0
                else:
                    if filtro_asc==True:
                        if (df['Filtro_value'][i]>=filtro_rsi and df['Filtro_value'][i]>=df['Filtro_value'][i-1]):
                            df['Filtro'][i]=1
                    else:
                        if df['Filtro_value'][i]>=filtro_rsi:
                            df['Filtro'][i]=1
            #END - filtros
            #START - Definicao de Custos, resto, Stock, Stock_acum
            if timeframe=='daily-weekday':
                if df['Count_Month'][i]<df['Count_Month'][i-1]:
                    if df['WeekDay'][i]<df['WeekDay'][i-1]:        
                        try:
                            df['resto'][i]=round(df['money_equiv'][i]+(df['resto'][i-1]*(1+df['cdi'][i])),2)
                        except:
                            df['resto'][i]=money
                    else:
                        try:
                            df['resto'][i]=round(df['resto'][i-1]*(1+df['cdi'][i]),2)
                        except:
                            df['resto'][i]=money
                else:
                    if df['WeekDay'][i]<df['WeekDay'][i-1]:        
                        try:
                            df['resto'][i]=round(df['money_equiv'][i]+df['resto'][i-1],2)
                        except:
                            df['resto'][i]=money
                    else:
                        try:
                            df['resto'][i]=df['resto'][i-1]
                        except:
                            df['resto'][i]=money
                        
                df['resto'][i]=round(df['resto'][i]+df['Dividends'][i]*df['stock_acum'][i-1],2)
                
                if df['WeekDay'][i]==pregao:
                    #Primeiro: faz uma estimativa de custos considerando que todo o resto vai ser usado        
                    df['custos'][i]=(round((1+(issqn/100))*corretagem,2)+(round(((df['resto'][i])*(bovespa/100)),2)))*df['Filtro'][i]
                    df['stock'][i]=np.floor(((df['resto'][i]-df['custos'][i])/df['Close'][i])*df['Filtro'][i])
                    #Segundo: faz o custo atualizado com a quantidade correta de ações compradas
                    df['custos'][i]=(round((1+(issqn/100))*corretagem,2)+(round((df['stock'][i]*df['Close'][i]*(bovespa/100)),2)))*df['Filtro'][i]
                    try:
                        df['resto'][i]=round((df['resto'][i]+(df['Dividends'][i]*df['stock_acum'][i-1])-df['custos'][i]-(df['stock'][i]*df['Close'][i])),2)
                    except:
                        df['resto'][i]=round((df['resto'][i]-df['custos'][i])-(df['stock'][i]*df['Close'][i]),2)
                    try:    
                        if df['Stock Splits'][i]!=0:
                            df['stock_acum'][i]=(df['stock'][i])+(df['stock_acum'][i-1]*df['Stock Splits'][i])
                        else:
                            df['stock_acum'][i]=df['stock'][i]+df['stock_acum'][i-1]
                    except:
                        df['stock_acum'][i]=df['stock'][i]
                else:
                    df['custos'][i]=0
                    try:    
                        if df['Stock Splits'][i]!=0:
                            df['stock_acum'][i]=(df['stock'][i])+(df['stock_acum'][i-1]*df['Stock Splits'][i])
                        else:
                            df['stock_acum'][i]=df['stock'][i]+df['stock_acum'][i-1]
                    except:
                        df['stock_acum'][i]=df['stock'][i]
                
            else:
                if pregao == 0: 
                    #Primeiro faz uma estimativa de custos considerando que todo o money_equiv vai ser usado        
                    df['custos'][i]=(round((1+(issqn/100))*corretagem,2)+(round(((df['money_equiv'][i]+df['resto'][i])*(bovespa/100)),2)))*df['Filtro'][i]
                    try:
                        df['stock'][i]=np.floor(((df['money_equiv'][i]+df['resto'][i-1]-df['custos'][i])/df['Close'][i])*df['Filtro'][i])
                    except:
                        df['stock'][i]=np.floor(((df['money_equiv'][i]-df['custos'][i])/df['Close'][i])*df['Filtro'][i])
                    #Segundo faz o custo atualizado com a quantidade correta de ações compradas
                    df['custos'][i]=(round((1+(issqn/100))*corretagem,2)+(round((df['stock'][i]*df['Close'][i]*(bovespa/100)),2)))*df['Filtro'][i]
                    try:
                        df['resto'][i]=round((df['money_equiv'][i]+(df['Dividends'][i]*df['stock_acum'][i-1])-df['custos'][i]+(df['resto'][i-1]*(1+df['cdi'][i-1]))-(df['stock'][i]*df['Close'][i])),2)
                    except:
                        df['resto'][i]=round((df['money_equiv'][i]-df['custos'][i])-(df['stock'][i]*df['Close'][i]),2)
                    try:    
                        if df['Stock Splits'][i]!=0:
                            df['stock_acum'][i]=(df['stock'][i])+(df['stock_acum'][i-1]*df['Stock Splits'][i])
                        else:
                            df['stock_acum'][i]=df['stock'][i]+df['stock_acum'][i-1]
                    except:
                        df['stock_acum'][i]=df['stock'][i]

                elif(pregao>0):
                    if df['Count_Month'][i]==pregao:
                        #Primeiro faz uma estimativa de custos considerando que todo o money_equiv vai ser usado        
                        df['custos'][i]=(round((1+(issqn/100))*corretagem,2)+(round(((df['money_equiv'][i]+df['resto'][i])*(bovespa/100)),2)))*df['Filtro'][i]
                        try:
                            df['stock'][i]=np.floor(((df['money_equiv'][i]+df['resto'][i-1]-df['custos'][i])/df['Close'][i])*df['Filtro'][i])
                        except:
                            df['stock'][i]=np.floor(((df['money_equiv'][i]-df['custos'][i])/df['Close'][i])*df['Filtro'][i])
                        #Segundo faz o custo atualizado com a quantidade correta de ações compradas
                        df['custos'][i]=(round((1+(issqn/100))*corretagem,2)+(round((df['stock'][i]*df['Close'][i]*(bovespa/100)),2)))*df['Filtro'][i]
                        try:
                            df['resto'][i]=round((df['money_equiv'][i]+(df['Dividends'][i]*df['stock_acum'][i-1])-df['custos'][i]+(df['resto'][i-1]*(1+df['cdi'][i-1]))-(df['stock'][i]*df['Close'][i])),2)
                        except:
                            df['resto'][i]=round((df['money_equiv'][i]-df['custos'][i])-(df['stock'][i]*df['Close'][i]),2)
                        try:    
                            if df['Stock Splits'][i]!=0:
                                df['stock_acum'][i]=(df['stock'][i])+(df['stock_acum'][i-1]*df['Stock Splits'][i])
                            else:
                                df['stock_acum'][i]=df['stock'][i]+df['stock_acum'][i-1]
                        except:
                            df['stock_acum'][i]=df['stock'][i]
                    else:
                        try:
                            df['stock'][i]=df['stock'][i-1]
                        except:
                            df['stock'][i]=0
                        df['custos'][i]=0
                        try:
                            df['resto'][i]=round((df['Dividends'][i]*df['stock_acum'][i-1])+df['resto'][i-1],2)
                        except:
                            df['resto'][i]=0.

                        try:
                            df['stock_acum'][i]=df['stock_acum'][i-1]
                        except:
                            df['stock_acum'][i]=0
                else:
                    if df['Count_Month_Rev'][i]==pregao:
                        #Primeiro faz uma estimativa de custos considerando que todo o money_equiv vai ser usado        
                        df['custos'][i]=(round((1+(issqn/100))*corretagem,2)+(round(((df['money_equiv'][i]+df['resto'][i])*(bovespa/100)),2)))*df['Filtro'][i]
                        try:
                            df['stock'][i]=np.floor(((df['money_equiv'][i]+df['resto'][i-1]-df['custos'][i])/df['Close'][i])*df['Filtro'][i])
                        except:
                            df['stock'][i]=np.floor(((df['money_equiv'][i]-df['custos'][i])/df['Close'][i])*df['Filtro'][i])
                        #Segundo faz o custo atualizado com a quantidade correta de ações compradas
                        df['custos'][i]=(round((1+(issqn/100))*corretagem,2)+(round((df['stock'][i]*df['Close'][i]*(bovespa/100)),2)))*df['Filtro'][i]
                        try:
                            df['resto'][i]=round((df['money_equiv'][i]+(df['Dividends'][i]*df['stock_acum'][i-1])-df['custos'][i]+(df['resto'][i-1]*(1+df['cdi'][i-1]))-(df['stock'][i]*df['Close'][i])),2)
                        except:
                            df['resto'][i]=round((df['money_equiv'][i]-df['custos'][i])-(df['stock'][i]*df['Close'][i]),2)
                        try:    
                            if df['Stock Splits'][i]!=0:
                                df['stock_acum'][i]=(df['stock'][i])+(df['stock_acum'][i-1]*df['Stock Splits'][i])
                            else:
                                df['stock_acum'][i]=df['stock'][i]+df['stock_acum'][i-1]
                        except:
                            df['stock_acum'][i]=df['stock'][i]
                    else:
                        try:
                            df['stock'][i]=df['stock'][i-1]
                        except:
                            df['stock'][i]=0
                        df['custos'][i]=0
                        try:
                            df['resto'][i]=round((df['Dividends'][i]*df['stock_acum'][i-1])+df['resto'][i-1],2)
                        except:
                            df['resto'][i]=0.

                        try:
                            df['stock_acum'][i]=df['stock_acum'][i-1]
                        except:
                            df['stock_acum'][i]=0          
            #END - Definicao de Custos, Stock, Stock_acum
            
            
    if return_type=='table':
        return df
    
    elif return_type.lower()=='summary' or return_type.lower() == 'row':
        Date_Start = df.iloc[0]['Date']
        Date_End = df.iloc[-1]['Date']
        Aporte = df.iloc[-1]['money_equiv']
        Total_Aportado = df.iloc[-1]['money_acum']
        Total_Acoes = df.iloc[-1]['stock_acum']
        Ultimo_Fechamento = df.iloc[-1]['Close']
        Caixa_Livre = df.iloc[-1]['resto']
        Custos_Total = df['custos'].sum()
        RendaFixa_Final = df.iloc[-1]['rf_acum']
        
        if (filtro.lower()=='none'):
            Filtro = "None"
        elif (filtro.lower() == 'rsi'):
            Filtro = "RSI("+str(filtro_periodos)+") "+filtro_sinal+str(filtro_rsi)+"; ASC:"+str(filtro_asc)
        elif filtro.lower() == 'sma':
            Filtro = filtro_sinal+" SMA("+str(filtro_periodos)+"); ASC:"+str(filtro_asc)
            
        if pregao == 0:
            periodos = df.shape[0]
            periodos_aport = df['Filtro'].sum()
        else:
            if timeframe=='daily-week-weekday':
                per=df.copy()
                per.set_index('Date', inplace=True, drop=True)
                per = per.resample('M').agg({'Count_Month': 'max'})
                periodos = per.shape[0] 
                periodos_aport = df[(df['Filtro']==1) & (df['Count_Month']==pregao)]['Count_Month'].count()
            # -----------------WIP -----------------------
            elif timeframe=='daily-weekday':
                per=df.copy()
                per.set_index('Date', inplace=True, drop=True)
                per = per.resample('W').agg({'Count_Month': 'max'})
                periodos = per.shape[0] 
                periodos_aport = df[(df['Filtro']==1) & (df['WeekDay']==pregao)]['Count_Month'].count()
            # -----------------WIP -----------------------
            else:
                if pregao > 0 :
                    periodos = df[df['Count_Month']==1]['Count_Month'].count()
                    periodos_aport = df[(df['Filtro']==1) & (df['Count_Month']==pregao)]['Count_Month'].count()
                else:
                    periodos = df[df['Count_Month_Rev']==-1]['Count_Month'].count()
                    periodos_aport = df[(df['Filtro']==1) & (df['Count_Month_Rev']==pregao)]['Count_Month'].count()
        
        
        if return_type.lower() == 'summary':
            data = {'Inicial': [pd.to_datetime(Date_Start).strftime('%d/%m/%Y')
                                , 0 
                                , real_br_money_mask(money)
                                , 0 
                                , 0
                                , 0
                                , 0
                                , 0
                                , 0
                                , 0
                                , 0
                                ,0 
                                ,''
                                ,''
                                ,''],
                    'Final': [pd.to_datetime(Date_End).strftime('%d/%m/%Y')
                                , periodos
                                , real_br_money_mask(Aporte)
                                , periodos_aport
                                , real_br_money_mask(Total_Aportado)
                                , int(Total_Acoes)
                                , real_br_money_mask(Total_Acoes*Ultimo_Fechamento)
                                , real_br_money_mask(Caixa_Livre)
                                , real_br_money_mask(Caixa_Livre+Total_Acoes*Ultimo_Fechamento)
                                , real_br_money_mask(Custos_Total)
                                , real_br_money_mask((Caixa_Livre+Total_Acoes*Ultimo_Fechamento)/int(periodos_aport))
                                , real_br_money_mask(RendaFixa_Final)
                                ,''
                                ,''
                                ,''],
                    'Porcentagem': ['NA'
                                , 'NA'      
                                , str(round((Aporte/money-1)*100,1))+" %"
                                , str(round(periodos_aport/periodos*100,1))+" %"  
                                , 'NA'
                                , 'NA'
                                , 'NA'
                                , 'NA'
                                , str(round(((Caixa_Livre+Total_Acoes*Ultimo_Fechamento-Total_Aportado)/Total_Aportado)*100,2))+" %"
                                , 'NA'
                                , 'NA'
                                , str(round(((RendaFixa_Final-Total_Aportado)/Total_Aportado)*100,2))+" %"
                                ,''
                                ,''
                                ,'']
            }

            summary = pd.DataFrame(data, index=['Data'
                                                , 'Periodos'
                                                , 'Aporte por Periodo'
                                                , 'Total vezes aportado'
                                                , 'Total $ aportado'
                                                , 'Total Ações'
                                                , 'Patrimonio em Ações'
                                                , 'Caixa Livre'
                                                , 'Patrimonio Total'
                                                , 'Custos'
                                                , 'Lucro por Aporte'
                                                , 'Renda Fixa '+str(rf_cdi)+' % CDI'
                                                , 'Timeframe: '+timeframe.capitalize()
                                                , 'Pregão: '+str(pregao)
                                                , 'Filtro: '+Filtro])

            return summary
        elif (return_type.lower()=="row"):
            row = pd.DataFrame({'Data_Inicial':[Date_Start]
            , 'Data_Final':[Date_End]
            , 'Total_Periodos': periodos
            , 'Aporte_Inicial':[money]
            , 'Aporte_Final':[Aporte]
            , 'Total_vezes_aportado':periodos_aport
            , 'Total_Aportado':[Total_Aportado]
            , 'Total_Acoes':[Total_Acoes]
            , 'Patrimonio_em_Acoes':[round(Total_Acoes*Ultimo_Fechamento,2)]
            , 'Caixa_Livre':[Caixa_Livre]
            , 'Patrimonio_Total': Caixa_Livre+Total_Acoes*Ultimo_Fechamento
            , 'Retorno_Trade': (Caixa_Livre+Total_Acoes*Ultimo_Fechamento)/int(periodos_aport)      
            , 'Renda_Fixa_Total':[RendaFixa_Final]
            , 'Corretagem':[corretagem]
            , 'ISSQN':[issqn]
            , 'Taxas B3':[bovespa]
            , 'RF CDI':[rf_cdi]
            , 'Filtro':[filtro]
            , 'Filtro_Periodos':[filtro_periodos]
            , 'Filtro_RSI':[filtro_rsi]
            , 'Filtro_ASC':[filtro_asc]
            , 'Pregao':[pregao]
                       })
            return row
    
    elif (return_type.lower()=="excel"):
        df.to_excel(r'C:\Users\cmtem\OneDrive\Área de Trabalho\aporte_recorrente_'+ticker+'.xlsx', index = False)
        
    else:
        return df
