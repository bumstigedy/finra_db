
import pandas as pd
import plotly
import plotly.graph_objects as go
import psycopg2
from plotly.subplots import make_subplots
import os
##############################################################
user = os.getenv('postgres_user')
pw=os.getenv('postgres_pw')
#

conn = psycopg2.connect(
    host="localhost",
    database="finra",  ###### name it
    user=user,   # MAKE Envir variable
    password=pw)
######################################################
def getData(conn):
    """ get data from database  """ 
    sql=""" SELECT "Date", SUM(COALESCE("VOLUME"*"weight",0)) AS "VOL" FROM
        (SELECT
                    "Date",
                    "Symbol",
                    SUM("ShortVolume") AS "VOLUME"
                FROM
                    otc
                GROUP BY
                    "Date",
                    "Symbol"
                ) qry1
        LEFT JOIN
            spx
            ON
            "qry1"."Symbol" = "spx"."Symbol"
        WHERE "spx"."Sector" IS NOT NULL	
        GROUP BY
            "Date"
            """
    df=pd.read_sql(sql,conn)
    return df

df2=getData(conn)    
df2=df2.sort_values(by='Date')


df2['12ema']=df2.VOL.ewm(span=12, adjust=False).mean()
df2['26ema']=df2.VOL.ewm(span=26, adjust=False).mean()
df2['PVO']=100*((df2['12ema']-df2['26ema'])/df2['26ema'])
df2['signal']=df2.PVO.ewm(span=9, adjust=False).mean()

fig = make_subplots(specs=[[{"secondary_y": True}]])
fig.add_trace(
                    go.Scatter(x=df2.Date, y=df2.PVO, name="PVO"),
                    secondary_y=False,
                )
fig.add_trace(
                    go.Scatter(x=df2.Date, y=df2.signal, name="signal line"),
                    secondary_y=False,
                )
fig.add_trace(
                    go.Bar(x=df2.Date, y=df2.PVO-df2.signal, name="PVO-signal", marker_color='gray'),
                    secondary_y=True,
                )

fig.update_layout(
        title="Volume and PVO",
        legend_title=" ",
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)'
    
        )
fig.update_yaxes(title_text="%", secondary_y=False)
fig.update_yaxes(title_text="volume", secondary_y=True)

plotly.offline.plot(fig,auto_open=True)

print(df2.head())