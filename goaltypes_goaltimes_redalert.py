import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

import os
import datetime
import json
import time
import requests
import betfairlightweight
import re


from betfairlightweight import filters
from bs4 import BeautifulSoup

pd.set_option('display.max_columns', 500)
pd.set_option('display.max_rows', 500)

######## DATA CRUNCHING FUNCTIONS ###########


def get_goal_types_Bundesliga():
    '''
    gets data from soccerstats Bundesliga and returns a df of selected stats
    '''
    
    url = 'https://www.soccerstats.com/table.asp?league=germany&tid=u'
    
    # bs scrape
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    mydivs = soup.findAll("div", {"class": "tabbertab tabbertabdefault"})
    data_frame = pd.read_html(str(mydivs))[0]
    
    # save for future usage
    data_frame.to_csv('goal_types_Bundesliga.csv')
    goal_types_Bundesliga = pd.read_csv('goal_types_Bundesliga.csv')
    
    # clean and make ready for analysis
    goal_types_Bundesliga.drop(['Unnamed: 0','2'], inplace=True, axis=1)
    
    goal_types_Bundesliga.columns = ['team_name', 'GP', 
                      't_equalize', 't_equalize%', 't_takelead', 't_takelead%','t_inc_dec_lead', 't_inc_dec_lead%',
                      'o_equalize', 'o_equalize%', 'o_takelead', 'o_takelead%','o_inc_dec_lead', 'o_inc_dec_lead%']
    
    goal_types_Bundesliga = goal_types_Bundesliga.drop([0,1]).set_index('team_name')
    goal_types_Bundesliga.drop(['t_equalize%','t_takelead%','t_inc_dec_lead%', 
                     'o_equalize%', 'o_takelead%', 'o_inc_dec_lead%'], 
                    inplace=True, axis=1)
    
    return goal_types_Bundesliga

def get_goal_timings_Bundesliga():
    '''
    gets data from soccerstats and returns a df of selected stats of goal timings

    '''
    
    url = 'https://www.soccerstats.com/timing.asp?league=germany'
    page = requests.get(url)
    
    #bs scrape
    soup = BeautifulSoup(page.content, 'html.parser')
    mydivs = soup.findAll("div", {"class": "tabbertab tabbertabdefault"})
    data_frame = pd.read_html(str(mydivs))[0]
    
    # save as csv
    data_frame.to_csv('goal_timing_Bundesliga.csv') 
    goal_timing_Bundesliga = pd.read_csv('goal_timing_Bundesliga.csv')
    #cleaning
    goal_timing_Bundesliga.drop(['Unnamed: 0','11', '10', '12'], inplace=True, axis=1)
    goal_timing_Bundesliga = goal_timing_Bundesliga.rename(columns=goal_timing_Bundesliga.iloc[0])
    goal_timing_Bundesliga = goal_timing_Bundesliga.drop([0, 19])
    goal_timing_Bundesliga = goal_timing_Bundesliga.rename(columns={"Overall": "team_name"})
    goal_timing_Bundesliga = goal_timing_Bundesliga.set_index('team_name')
    
    return goal_timing_Bundesliga



def make_goal_types_timings_Bundesliga_home(goal_types_Bundesliga, goal_timing_Bundesliga):
    '''
    input: goal types and goal timings dataframes 
    output: a df of cleaned and concatted from input
    '''
    # cleaning
    df = pd.concat([goal_types_Bundesliga, goal_timing_Bundesliga], axis=1)
    df[['Ht_0110','Ho_0110']] = df['0-10'].str.split("-",expand=True) 
    df[['Ht_1120','Ho_1120']] = df['11-20'].str.split("-",expand=True) 
    df[['Ht_2130','Ho_2130']] = df['21-30'].str.split("-",expand=True) 
    df[['Ht_3140','Ho_3140']] = df['31-40'].str.split("-",expand=True) 
    df[['Ht_4150','Ho_4150']] = df['41-50'].str.split("-",expand=True) 
    df[['Ht_5160','Ho_5160']] = df['51-60'].str.split("-",expand=True) 
    df[['Ht_6170','Ho_6170']] = df['61-70'].str.split("-",expand=True) 
    df[['Ht_7180','Ho_7180']] = df['71-80'].str.split("-",expand=True) 
    df[['Ht_8190','Ho_8190']] = df['81-90'].str.split("-",expand=True) 
    df = df.drop(columns=['0-10','11-20','21-30','31-40','41-50','51-60','61-70','71-80','81-90'])
    
    
    df["t_takelead"] = df["t_takelead"].astype(int)
    df["o_takelead"] = df["o_takelead"].astype(int)
    df["o_inc_dec_lead"] = df["o_inc_dec_lead"].astype(int)
    df["t_inc_dec_lead"] = df["t_inc_dec_lead"].astype(int)
    df["GP"] = df["GP"].astype(int)
    
    df["Ht_0110"] = df["Ht_0110"].astype(int)
    df["Ho_0110"] = df["Ho_0110"].astype(int)
    df["Ht_1120"] = df["Ht_1120"].astype(int)
    df["Ho_1120"] = df["Ho_1120"].astype(int)
    df["Ht_2130"] = df["Ht_2130"].astype(int)
    df["Ho_2130"] = df["Ho_2130"].astype(int)
    df["Ht_3140"] = df["Ht_3140"].astype(int)
    df["Ho_3140"] = df["Ho_3140"].astype(int)
    df["Ht_4150"] = df["Ht_4150"].astype(int)
    df["Ho_4150"] = df["Ho_4150"].astype(int)
    df["Ht_5160"] = df["Ht_5160"].astype(int)
    df["Ho_5160"] = df["Ho_5160"].astype(int)
    df["Ht_6170"] = df["Ht_6170"].astype(int)
    df["Ho_6170"] = df["Ho_6170"].astype(int)
    df["Ht_7180"] = df["Ht_7180"].astype(int)
    df["Ho_7180"] = df["Ho_7180"].astype(int)
    df["Ho_8190"] = df["Ho_8190"].astype(int)
    df["Ht_8190"] = df["Ht_8190"].astype(int)
    df["t_equalize"] = df["t_equalize"].astype(int)
    df["o_equalize"] = df["o_equalize"].astype(int)
    
    return df


def make_goal_types_timings_Bundesliga_away(goal_types_Bundesliga, goal_timing_Bundesliga):
    '''
    input: goal types and goal timings dataframes 
    output: a df of cleaned and concatted from input
    '''
    # cleaning
    df = pd.concat([goal_types_Bundesliga, goal_timing_Bundesliga], axis=1)
    df[['At_0110','Ao_0110']] = df['0-10'].str.split("-",expand=True) 
    df[['At_1120','Ao_1120']] = df['11-20'].str.split("-",expand=True) 
    df[['At_2130','Ao_2130']] = df['21-30'].str.split("-",expand=True) 
    df[['At_3140','Ao_3140']] = df['31-40'].str.split("-",expand=True) 
    df[['At_4150','Ao_4150']] = df['41-50'].str.split("-",expand=True) 
    df[['At_5160','Ao_5160']] = df['51-60'].str.split("-",expand=True) 
    df[['At_6170','Ao_6170']] = df['61-70'].str.split("-",expand=True) 
    df[['At_7180','Ao_7180']] = df['71-80'].str.split("-",expand=True) 
    df[['At_8190','Ao_8190']] = df['81-90'].str.split("-",expand=True) 
    df = df.drop(columns=['0-10','11-20','21-30','31-40','41-50','51-60','61-70','71-80','81-90'])
    
    
    df["t_takelead"] = df["t_takelead"].astype(int)
    df["o_takelead"] = df["o_takelead"].astype(int)
    df["o_inc_dec_lead"] = df["o_inc_dec_lead"].astype(int)
    df["t_inc_dec_lead"] = df["t_inc_dec_lead"].astype(int)
    df["GP"] = df["GP"].astype(int)
    
    df["At_0110"] = df["At_0110"].astype(int)
    df["Ao_0110"] = df["Ao_0110"].astype(int)
    df["At_1120"] = df["At_1120"].astype(int)
    df["Ao_1120"] = df["Ao_1120"].astype(int)
    df["At_2130"] = df["At_2130"].astype(int)
    df["Ao_2130"] = df["Ao_2130"].astype(int)
    df["At_3140"] = df["At_3140"].astype(int)
    df["Ao_3140"] = df["Ao_3140"].astype(int)
    df["At_4150"] = df["At_4150"].astype(int)
    df["Ao_4150"] = df["Ao_4150"].astype(int)
    df["At_5160"] = df["At_5160"].astype(int)
    df["Ao_5160"] = df["Ao_5160"].astype(int)
    df["At_6170"] = df["At_6170"].astype(int)
    df["Ao_6170"] = df["Ao_6170"].astype(int)
    df["At_7180"] = df["At_7180"].astype(int)
    df["Ao_7180"] = df["Ao_7180"].astype(int)
    df["Ao_8190"] = df["Ao_8190"].astype(int)
    df["At_8190"] = df["At_8190"].astype(int)
    df["t_equalize"] = df["t_equalize"].astype(int)
    df["o_equalize"] = df["o_equalize"].astype(int)
    
    return df





def make_metrics_home(df):
    '''
    input: concatted goal types and timings Bundesliga2
    output: Bundesliga2 with metrics
    '''
    df['H_buyuk_inat'] = (df['t_equalize'])*100 / (df['o_takelead'])
    df['H_kucuk_inat'] = (df['t_equalize'])*100 / (df['GP'])
    df['H_buyuk_sal'] = (df['o_equalize'])*100 / (df['t_takelead'])
    df['H_kucuk_sal'] = (df['o_equalize'])*100 / (df['GP'])
    df['H_one_gec'] = ((df['t_takelead']) + (df['o_takelead']))*100 / (df['GP'])
    df['H_farki_ac'] = ((df['t_inc_dec_lead']) + (df['o_inc_dec_lead']))*100 / (df['GP'])
 
    
    df = df[['GP','H_buyuk_inat', 'H_kucuk_inat', 'H_buyuk_sal', 'H_kucuk_sal','H_one_gec','H_farki_ac',
         "Ht_0110","Ho_0110","Ht_1120","Ho_1120","Ht_2130","Ho_2130","Ht_3140","Ho_3140","Ht_4150","Ho_4150","Ht_5160","Ho_5160",
          'Ht_6170', 'Ho_6170', 'Ht_7180', 'Ho_7180',
          'Ht_8190', 'Ho_8190']]
    
    
    df = df.round(1)

    return df 



def make_metrics_away(df):
    '''
    input: concatted goal types and timings Bundesliga2
    output: Bundesliga2 with metrics
    '''
    df['A_buyuk_inat'] = (df['t_equalize'])*100 / (df['o_takelead'])
    df['A_kucuk_inat'] = (df['t_equalize'])*100 / (df['GP'])
    df['A_buyuk_sal'] = (df['o_equalize'])*100 / (df['t_takelead'])
    df['A_kucuk_sal'] = (df['o_equalize'])*100 / (df['GP'])
    df['A_one_gec'] = ((df['t_takelead']) + (df['o_takelead']))*100 / (df['GP'])
    df['A_farki_ac'] = ((df['t_inc_dec_lead']) + (df['o_inc_dec_lead']))*100 / (df['GP'])
 
    
    df = df[['GP','A_buyuk_inat', 'A_kucuk_inat', 'A_buyuk_sal', 'A_kucuk_sal','A_one_gec','A_farki_ac',
         "At_0110","Ao_0110","At_1120","Ao_1120","At_2130","Ao_2130","At_3140","Ao_3140","At_4150","Ao_4150","At_5160","Ao_5160",
          'At_6170', 'Ao_6170', 'At_7180', 'Ao_7180',
          'At_8190', 'Ao_8190']]
    
    
    df = df.round(1)

    return df




