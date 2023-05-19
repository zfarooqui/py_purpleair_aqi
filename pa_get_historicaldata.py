# -*- coding: utf-8 -*-
"""
This code gets hisotrical PurpleAir data of one site at a time and 
for two days ONLY from new PurpleAir API.

Data from the site are in bytes/text and NOT in JSON format.

Created on Fri Jun 10 21:34:01 2022

@author: Zuber Farooqui, Ph.D.
"""

import requests
import pandas as pd
from datetime import datetime
import time
import json
from io import StringIO
from sqlalchemy import create_engine

# Starting engine for postgresql
engine = create_engine('postgresql://postgres:password@location:port/database')

# API Keys provided by PurpleAir(c)
key_read  = 'insert your key'

# Sleep Seconds
sleep_seconds = 3 # wait sleep_seconds after each query

def get_sensorslist(nwlng,nwlat,selng,selat,location,key_read):
    # PurpleAir API URL
    root_url = 'https://api.purpleair.com/v1/sensors/'

    # Box domain: lat_lon = [nwlng,, nwlat, selng, selat]
    lat_lon = [nwlng, nwlat, selng, selat]
    for i,l in enumerate(lat_lon):
        if (i == 0):
            ll_api_url = f'&nwlng={l}'
        elif (i == 1):
            ll_api_url += f'&nwlat={l}'
        elif (i == 2):
            ll_api_url += f'&selng={l}'
        elif (i == 3):
            ll_api_url += f'&selat={l}'
        
    # Fields to get
    fields_list = ['sensor_index','name','latitude','longitude','location_type'] 
    for i,f in enumerate(fields_list):
        if (i == 0):
            fields_api_url = f'&fields={f}'
        else:
            fields_api_url += f'%2C{f}'

    # Indoor, outdoor or all
    if (location == 'indoor'):
        loc_api = f'&location_type=1'
    elif (location == 'outdoor'):
        loc_api = f'&location_type=0'
    else:
        loc_api = ''
            
    # Final API URL
    api_url = root_url + f'?api_key={key_read}' + fields_api_url + ll_api_url + loc_api

    # Getting data
    response = requests.get(api_url)

    if response.status_code == 200:
        #print(response.text)
        json_data = json.loads(response.content)['data']
        df = pd.DataFrame.from_records(json_data)
        df.columns = fields_list
    else:
        raise requests.exceptions.RequestException

    # Creating a PurpleAir monitors table in PostgreSQL (Optional)
    #      If you dont want to save to PostgreSQL then comment line 22 and 78
    df.to_sql('tablename', con=engine, if_exists='append', index=False)
    
    # writing to csv file
    folderpath = 'Folder path'
    filename = folderpath + '\sensors_list.csv'
    df.to_csv(filename, index=False, header=True)
            
    # Creating a Sensors 
    sensorslist = list(df.sensor_index)
    
    return sensorslist

def get_historicaldata(sensors_list,bdate,edate,average_time,key_read):
    # Historical API URL
    root_api_url = 'https://api.purpleair.com/v1/sensors/'
    
    # Average time: The desired average in minutes, one of the following:0 (real-time),10 (default if not specified),30,60
    average_api = f'&average={average_time}'

    # Creating fields api url from fields list to download the data: Note: Sensor ID/Index will not be downloaded as default
    fields_list = ['pm2.5_atm_a', 'pm2.5_atm_b', 'pm2.5_cf_1_a', 'pm2.5_cf_1_b', 'humidity_a', 'humidity_b', 
               'temperature_a', 'temperature_b', 'pressure_a', 'pressure_b']
    for i,f in enumerate(fields_list):
        if (i == 0):
            fields_api_url = f'&fields={f}'
        else:
            fields_api_url += f'%2C{f}'

    # Dates of Historical Data period
    begindate = datetime.fromisoformat(bdate)
    enddate   = datetime.fromisoformat(edate)
    
    # Downlaod days based on average
    if (average_time == 60):
        datelist = pd.date_range(begindate,enddate,freq='14d') # for 14 days of data
    else:
        datelist = pd.date_range(begindate,enddate,freq='2d') # for 2 days of data
        
    # Reversing to get data from end date to start date
    datelist = datelist.tolist()
    datelist.reverse()
    
    # Converting to PA required format
    date_list=[]
    for dt in datelist:
        dd = dt.strftime('%Y-%m-%d') + 'T' + dt.strftime('%H:%M:%S') +'Z'
        date_list.append(dd)

    # to get data from end date to start date
    len_datelist = len(date_list) - 1
        
    # Getting 2-data for one sensor at a time
    for s in sensors_list:
        # Adding sensor_index & API Key
        hist_api_url = root_api_url + f'{s}/history/csv?api_key={key_read}'

        # Creating start and end date api url
        for i,d in enumerate(date_list):
            # Wait time 
            time.sleep(sleep_seconds)
            
            if (i < len_datelist):
                print('Downloading for PA: %s for Dates: %s and %s.' %(s,date_list[i+1]),d))
                dates_api_url = f'&start_timestamp={date_list[i+1]}&end_timestamp={d}'
            
                # Final API URL
                api_url = hist_api_url + dates_api_url + average_api + fields_api_url
                            
                #
                try:
                    response = requests.get(api_url)
                except:
                    print(api_url)
                #
                try:
                    assert response.status_code == requests.codes.ok
                
                    # Creating a Pandas DataFrame
                    df = pd.read_csv(StringIO(response.text), sep=",", header=0)
                
                except AssertionError:
                    df = pd.DataFrame()
                    print('Bad URL!')
            
                if df.empty:
                    print('------------- No Data Available -------------')
                else:
                    # Adding Sensor Index/ID
                    #df['id'] = s
                
                    # Dropping duplicate rows
                    df = df.drop_duplicates(subset=None, keep='first', inplace=False)
                    
                    # Writing to Postgres Table (Optional)
                    #      If you dont want to save to PostgreSQL then comment line 22, 78, and 173
                    df.to_sql('tablename', con=engine, if_exists='append', index=False)
                    
                    # writing to csv file
                    folderpath = 'Folder path'
                    filename = folderpath + '\sensorsID_%s_%s_%s.csv' % (s,date_list[i+1]),d)
                    df.to_csv(filename, index=False, header=True)

# Data download period
bdate = '2022-06-01T00:00:00+00:00' 
edate = '2022-06-15T00:00:00+00:00'

# Getting sensors list in Box domain [nwlng,, nwlat, selng, selat]
location='outdoor' # or 'indoor' or 'both'
sensors_list = get_sensorslist(65.001, 37.001, 99.001, 5.001, location, key_read)


# Average_time. The desired average in minutes, one of the following: 0 (real-time), 
#                  10 (default if not specified), 30, 60, 360 (6 hour), 1440 (1 day)
average_time=60 # or 10  or 0 (Current script is set only for real-time, 10, or 60 minutes data)

# Getting PA data
get_historicaldata(sensors_list,bdate,edate,average_time,key_read)
