import re
import requests
import pandas as pd
import io
from sqlalchemy import create_engine
import psycopg2
import datetime as dt
import os
#
user = os.getenv('postgres_user')
pw=os.getenv('postgres_pw')
#

conn = psycopg2.connect(
    host="localhost",
    database="finra",  ###### name it
    user=user,   # MAKE Envir variable
    password=pw)

##########################################################################################
def extract_links(txt_file): # input is location of text file with links from FINRA website
    """ parses text file to extract urls """
    file1 = open(txt_file, 'r')
    Lines= file1.readlines()
    links=[]  # empty list to hold url for each file
    for line in Lines:  # loop through eadh line of the file, and accumulate the links
        temp=(re.findall('"([^"]*)"', line))   # regex to extract the url inside quotes
        try:
            links.append(temp[0])    # comes  back as a list so get the first element
        except:
            pass
    
    return links # list with all of the urls


##################################################################################################
def download_data(url):
    """ downloads data for a given url into a df """
    r=requests.get(url)
    print(r.status_code)
    result=r.content
    decoded=io.StringIO(result.decode('utf-8'))
    df=pd.read_csv(decoded,sep='|')
    df.Date=df.Date.apply(lambda x: str(x))
    df=df.dropna()
    df.Date=pd.to_datetime(df.Date,format='%Y%m%d')
    if "CNMS" in url:
        df['source']="CNMS"
    if "FNQC" in url:
        df['source']="FNQC"
    if "FNRA" in url:
        df['source']="FNRA" 
    if "FNSQ" in url:
        df['source']="FNSQ"   
    if "FNYX" in url:
        df['source']="FNYX"
    if "FORF" in url:
        df['source']="FORF"    
    return df # df with data from one url
#####################################################################################################
def stuff_data(df,conn): # input is a dataframe
    """ inserts data into postgresql database """
    engine = create_engine('postgresql+psycopg2://postgres:postgres@localhost/finra')
    try:
        df.to_sql('otc',con=engine,index=False, if_exists='append')
    except:
        pass
####################################################################################################### 
def update_db(txt_file,db_dates): # location of the text file
    """ updates the database given the input file """  
    links= extract_links(txt_file) 
    df_test=pd.DataFrame()
    for link in links:
        print(link)
        try:
            df_temp=download_data(link)
            if df_temp.iloc[0].Date not in db_dates:  # check to make sure data is not already in database
                stuff_data(df_temp,conn)
                
        except:
            pass        
    return ""        
       
def getdates(conn):
    """ get dates already in database  """ 
    sql=""" SELECT DISTINCT "Date" FROM otc """
    df_dates=pd.read_sql(sql,conn)
    return df_dates.Date.unique()

db_dates=getdates(conn)
txt_file= r'C:\users\bumst\code\finra6.txt'  

update_db(txt_file,db_dates)
