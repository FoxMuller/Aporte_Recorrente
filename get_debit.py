#!/usr/bin/env python
# coding: utf-8

# ### get_debit(tipo)  / 'selic', 'ipca', 'cdi'
# 
# #### Version 0.12 
# - Adicionado SELIC
# 
# #### Version 0.11
# - IPCA, CDI

import requests
import pandas as pd
from bs4 import BeautifulSoup

def get_debit(tipo):
    tipo = tipo.lower()
    div = 1
    
    url="https://www.debit.com.br/tabelas/tabela-completa.php?indice="+tipo
    pagina_html = requests.get(url)
    pagina_html = BeautifulSoup(pagina_html.content, "html.parser")
    tabelas = pagina_html.find_all("table")
    df=[]
    df_prov=[]

    df=pd.read_html(str(tabelas[1]))[0]
    df=df.rename(columns = {'Data':'Date','%':tipo})
    df['Date']=pd.to_datetime(df['Date'],format='%m/%Y')
    df.set_index('Date', inplace=True)

    for i in range(len(tabelas)+1): 
        if i % 2 != 0 and i>1:
            df_prov=pd.read_html(str(tabelas[i]))[0]
            df_prov=df_prov.rename(columns = {'Data':'Date','%':tipo})
            df_prov['Date']=pd.to_datetime(df_prov['Date'],format='%m/%Y')
            df_prov.set_index('Date', inplace=True)
            df=df.append(df_prov)
    
    if tipo=='cdi':
        div=1000000
    elif tipo=='ipca':
        div=10000
    elif tipo=='selic':
        div=1000000
        df.rename(columns = {'Valor':tipo}, inplace=True)
    else:
        div=1
    
    return df/div