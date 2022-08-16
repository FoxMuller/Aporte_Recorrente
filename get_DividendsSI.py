#!/usr/bin/env python
# coding: utf-8

# ### get_DividendsSI(Ticker)  / 'Ticker'
# 
#### Version 0.11
#- Works with stocks, fii, and fiagros

#### Version 0.10
#- Only works for FII

import requests
import pandas as pd
from bs4 import BeautifulSoup

def get_DividendsSI(Ticker,fii):
    Ticker=Ticker.upper()
    if Ticker[-3:]==".SA":
        Ticker=Ticker.replace(".SA","")
        
    if fii==True:
        try: 
            url="https://statusinvest.com.br/fundos-imobiliarios/"+Ticker
            pagina_html = requests.get(url)
            pagina_html = BeautifulSoup(pagina_html.content, "html.parser")
            tabelas = str(pagina_html.find('input',id='results'))
            tabelas = tabelas.split("]\'>")[0]
            tabelas = tabelas[57:]
            json = pd.read_json(tabelas, lines=True)
            json['pd']=pd.to_datetime(json['pd'],format='%d/%m/%Y')

        except:
            url="https://statusinvest.com.br/fiagros/"+Ticker
            pagina_html = requests.get(url)
            pagina_html = BeautifulSoup(pagina_html.content, "html.parser")
            tabelas = str(pagina_html.find('input',id='results'))
            tabelas = tabelas.split("]\'>")[0]
            tabelas = tabelas[57:]
            json = pd.read_json(tabelas, lines=True)
            json['pd']=pd.to_datetime(json['pd'],format='%d/%m/%Y')  
    
    else:
        url="https://statusinvest.com.br/acoes/"+Ticker
        pagina_html = requests.get(url)
        pagina_html = BeautifulSoup(pagina_html.content, "html.parser")
        tabelas = str(pagina_html.find('input',id='results'))
        tabelas = tabelas.split("]\'>")[0]
        tabelas = tabelas[57:]
        json = pd.read_json(tabelas, lines=True)
        json['pd']=pd.to_datetime(json['pd'],format='%d/%m/%Y')
    
    json['sv']=json['sv'].str.replace(',', '.').astype(float)
    json.drop(columns=['y', 'm','d','ad','ed','et','etd','ov','sv','sov','adj'], inplace=True)
    json.rename(columns={"pd": "Date", "v": "Dividends"}, inplace=True)
    json.set_index('Date', inplace=True)
    json = json.iloc[::-1]
    return json





