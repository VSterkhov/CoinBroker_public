from pyhive import hive
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
warnings.filterwarnings("ignore")

# Выводим корреляционную матрицу
def show_correlation_map(df):
    cor = df.corr()
    plt.figure(figsize=(16,9))
    sns.heatmap(cor, center=0, cmap="RdYlGn", annot = True)
    plt.show()

def to_short_col_name(df, table_name):
    cols = list(df.columns)
    for col in cols: 
        df = df.rename(columns=({col: col[len(table_name):]}))
    return df
    
def split_dataframe_by_column(df, split_col, sort_col): 
    hash_map = {}
    for key in df[split_col].unique():
        hash_map[key] = df[df[split_col]==key].drop(split_col, axis=1).sort_values(by=[sort_col], ascending=True).reset_index(drop=True)
    return hash_map

connection = hive.Connection(host="localhost", port=10000, database="coin_broker")


trade_volume_df = pd.read_sql("SELECT * FROM coin_broker.trade_volume", connection)
trade_volume_df = to_short_col_name(trade_volume_df, 'trade_volume.').drop('current_day', axis=1)

trade_volume_map = split_dataframe_by_column(trade_volume_df, 'currency', 'current_time')


with plt.style.context('bmh'):
    plt.figure(figsize=(32, 20))
    x,y = 1,1
    for i, key in enumerate(trade_volume_map.keys()):
        ts_ax = plt.subplot2grid((5,4), (x, y))
        #div = trade_volume_map[key]['last_cost'] - trade_volume_map[key]['first_cost']
        #div.plot(ax=ts_ax, color='green')
        #trade_volume_map[key]['first_cost'].plot(ax=ts_ax, color='black')
        #trade_volume_map[key]['bid_avg_quantity'].plot(ax=ts_ax, color='blue')
        #trade_volume_map[key]['ask_avg_quantity'].plot(ax=ts_ax, color='red')
    
        trade_volume_map[key]['sell_avg_quantity'].map(lambda x:-x).plot(ax=ts_ax, color='purple')
        trade_volume_map[key]['buy_avg_quantity'].plot(ax=ts_ax, color='orange')

        ts_ax.set_title(key)
        
        if x==4:
            x=1
            y=y+1
        else:
            x=x+1
    plt.tight_layout()

'''

show_correlation_map(trade_volume_map['BTCUSDT'])




import numpy as np
from statsmodels.tsa.stattools import adfuller
import statsmodels.api as sm
import statsmodels.tsa.api as smt


################
 # Dickey-Fuller
##################
def test_stationarity(timeseries):
    print('Results of Dickey-Fuller Test:')
    dftest = adfuller(timeseries, autolag='AIC')
    dfoutput = pd.Series(dftest[0:4], index=['Test Statistic', 'p-value', '#Lags Used', 'Number of Observations Used'])
    for [key, value] in dftest[4].items():
        dfoutput['Critical Value (%s)' % key] = value
    print(dfoutput)

def tsplot(y, lags=None, figsize=(14, 8), style='bmh'):
    test_stationarity(y)
    if not isinstance(y, pd.Series):
        y = pd.Series(y)
    with plt.style.context(style):
        plt.figure(figsize=figsize)
        layout = (5, 1)
        ts_ax = plt.subplot2grid(layout, (0, 0), rowspan=2)
        acf_ax = plt.subplot2grid(layout, (2, 0))
        pacf_ax = plt.subplot2grid(layout, (3, 0))
        qq_ax = plt.subplot2grid(layout, (4, 0))

        y.plot(ax=ts_ax, color='blue', label='Or')
        ts_ax.set_title('Original')

        smt.graphics.plot_acf(y, lags=lags, ax=acf_ax, alpha=0.05)
        smt.graphics.plot_pacf(y, lags=lags, ax=pacf_ax, alpha=0.05)
        sm.qqplot(y, line='s', ax=qq_ax)
        
        plt.tight_layout()
    return


X = trade_volume_map['BTCUSDT']['first_cost']

best_aic = np.inf 
best_order = None
best_mdl = None

for i in range(5):
    for d in range(5):
        for j in range(5):
            try:
                tmp_mdl = smt.ARIMA(X, order=(i,d,j)).fit(method='innovations_mle')
                tmp_aic = tmp_mdl.aic
                if tmp_aic < best_aic:
                    best_aic = tmp_aic
                    best_order = (i, d, j)
                    best_mdl = tmp_mdl
            except: continue


print('aic: {:6.5f} | order: {}'.format(best_aic, best_order))


tsplot(best_mdl.resid, lags=30)

plt.figure(figsize=(14,8))
ax = plt.axes()
predictions = best_mdl.predict(1, len(X)+1000, ax=ax)
plt.plot(predictions, color='red', label='Predictions')
plt.plot(X, color='blue', label='X')
plt.legend()
plt.show()

'''
connection.close()