import Utils
import pandas as pd
import yfinance as yf
import requests
import json
import os
from MarketUniverse import MarketUniverse
from IndicatorCalculator import IndicatorAdder  # Fix the import
from DataFrameWindowFilter import DataWindowFilter

class MomentumStrategy:
    def __init__(self) -> None:
        self.symboluniverse = MarketUniverse()
        self.indicatorCalculator = IndicatorAdder()
        self.dataframewindowfilter = DataWindowFilter
    
    
    

if __name__ == "__main__":
    strategy = MomentumStrategy()
    symbolListDataframe = strategy.symboluniverse.optionBackedEquityUniverse()
    
    dirname = Utils.find_current_directory() +  "/" + "symbolSchemaFile"
    Utils.createDirectory(dirname)
    Utils.save_df_to_csv(symbolListDataframe, dirname, 'equitySchema.csv')
    
    symbolList = Utils.panda_series_toList_converter(symbolListDataframe['Symbol'])
    Utils.download_Mkt_Data_For_Symbols_And_SaveFilesToDirectory(symbolList, "2y", "1d", dirname, ".csv")

    for symbol in symbolList:
    #print(symbol)
        pathToFile = dirname + "/" +  symbol + ".NS.csv"
        if os.path.exists(pathToFile) == False:
            continue
        
        symbolDataframe = pd.read_csv(pathToFile)
        symbolDataframe = Utils.convert_yf_datetime_to_pandas_datetime(symbolDataframe)
    #     symbolData.dtypes
    #     # Add a new column 'formatted_date' in 'dd-mm-yyyy' format
    #     symbolData['formatted_date'] = symbolData['Date'].dt.strftime('%d-%m-%Y')
        Utils.convert_datetime64_to_user_format(symbolDataframe['Date'], '%d-%m-%Y')
        symbolDataframe = Utils.convert_yf_datetime_to_pandas_datetime(symbolDataframe)
        print(symbolDataframe.dtypes)
    #     symbolData['Date'] = pd.to_datetime(symbolData['Date'])
    #     symbolData.tail(5)
        start_date = '2024-03-07'
    #     #2023-09-07 00:00:00
        end_date = '2024-08-31'
    #     filtered_df = symbolData[(symbolData['Date'] >= start_date) & (symbolData['Date'] <= end_date)].copy()

        filteredSymbolDataFrame = Utils.filter_panda_df_in_range_constraint(symbolDataframe, 'Date', start_date, end_date)
    #     # symbolData
    #     filtered_df
    #     #symbolData.dtypes
    #     #start_date = pd.to_datetime('2023-09-07')
    #     #2023-09-07 00:00:00
    #     #end_date = pd.to_datetime('2023-09-07')
    #     filtered_df['Pct_Change'] = filtered_df['Close'].pct_change() * 100
    #     #filtered_df.loc[filtered_df['Close'].pct_change() * 100, 'Pct_Change']
    #     #filtered_df.loc[:, 'Pct_Change'] = filtered_df['Close'].pct_change() * 100
        filteredSymbolDataFrame = Utils.remove_timeZoneInfo_from_datetime(filteredSymbolDataFrame, 'Date')
        filteredSymbolDataFrame['YearMonth'] = Utils.convert_datetime_to_yearmonth(filteredSymbolDataFrame, 'Date')
        filteredSymbolDataFrame = strategy.indicatorCalculator.addAdvancesDeclinesIndicator(filteredSymbolDataFrame, 'Close', 'Advance-Decline')

        filteredSymbolDataFrame = Utils.generateDiffChangeFoGivenColumn(filteredSymbolDataFrame, 'Diff', 'Close')
        Utils.set_dataframe_value_for_index_and_columnName(filteredSymbolDataFrame, 0, 'Diff', 0)
        aa = strategy.dataframewindowfilter.get_calendar_month_end_filter_list(filteredSymbolDataFrame)
        bb = strategy.dataframewindowfilter.get_calendar_month_start_filter_list(filteredSymbolDataFrame)
        cc = Utils.create_union_of_two_pylist(aa, bb)
        print(filteredSymbolDataFrame.loc[:, ['Date', 'Close', 'Advance-Decline', 'Diff']])

        ss = Utils.filter_pands_df_column_filter_on_conditionlist(filteredSymbolDataFrame, 'Date', cc)
        print(ss.dtypes)
        #print(ss.loc[:, ['Date', 'Close', 'Advance-Decline', 'Diff', 'YearMonth']])

        #ss['Close_Diff'] = ss.groupby('YearMonth')['Close'].diff()
        #print(ss.loc[:, ['Date', 'Close', 'Advance-Decline', 'Diff', 'YearMonth', 'Close_Diff']])
        
        break
    #   
    # filtered_df.tail(10)
        
    #     # # Extract month and year
    #     filtered_df['Date'] = filtered_df['Date'].dt.tz_localize(None)
    #     filtered_df['YearMonth'] = filtered_df['Date'].dt.to_period('M').copy()
    #     filtered_df
    #     #symbolData.head(3)
    #     #symbolData.tail(3)
        
    #     # Calculate cumulative percentage change for each month
    #     aa = filtered_df.groupby('YearMonth')['Pct_Change'].cumsum()

    #     bb = filtered_df[['YearMonth', 'Pct_Change']]
    #     monthlySum = (bb.groupby('YearMonth').sum())
    #     #print(monthlySum)

    #     monthlySum.loc[:, 'Pct_Change'] = 1+(monthlySum['Pct_Change']/100)
    #     #print(monthlySum)
    #     # Calculate the product of all values in the 'Pct_Change' column
    #     product = float(monthlySum['Pct_Change'].prod()-1)
    #     #print(product)
    #     p[symbol] = (product*100)
    #     #bb.loc[:, 'Pct_Change'] = bb['Pct_Change']+1
    #     #print(p)
    #     #print(bb.groupby('YearMonth').sum())

    
    #     #p[symbol] = float(bb.groupby('YearMonth').sum().sum()['Pct_Change'])

    # #p

    # # Sorting the dictionary by float values
    # sorted_data = dict(sorted(p.items(), reverse=True, key=lambda item: item[1]))
    # sorted_data
    # cc = pd.DataFrame(list(sorted_data.items()), columns=['Stock', 'Return'])
    

