import pandas as pd 
import yfinance as yf
import yfinance
import os
import requests

'''
For the given location return the csv file format to a pandas dataframe
'''
def read_csvFile_to_dataframe(fileLocation):
    aa = pd.read_csv(fileLocation)
    return aa

'''
Save the given dataframe object to csv format in the location
'''
def save_df_to_csv(df, dirLocation, filename):
    df.to_csv(dirLocation + "/" + filename)

'''
Create Dataframe from python dictionary
'''
def create_Dataframe_From_PyDictionary_ListValues(dictionary):
    return pd.DataFrame(dictionary)

'''
Take the input as dataframe and columnName
and for the given columnName extract the UniqueElements in a series format
'''
def dataFrame_to_uniqueList_for_Column(dataframe, columnName):
    return dataframe[columnName].unique()


'''
Take a Dataframe Series and convert the series to Python List type
'''
def panda_series_toList_converter(dataframeSeries):
    return dataframeSeries.tolist()

'''
For the given symbol name extract the historical price data
from yahoo finance with the given period and interval in string format
e.g 
period : str Valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max

interval : str Valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
Intraday data cannot extend last 60 days
'''
def read_Historical_Data_From_Yf_with_period(symbolName, periodInString, intervalInString):
    try:
        stockTicker = yfinance.Ticker(symbolName)
        df = stockTicker.history(period=periodInString, interval=intervalInString)
        if df.empty:
            print("No data returned, possibly due to invalid parameters.")
        return df

    except Exception as e:
        print(f"Error fetching data: {e}")
        return None

'''
interval : str Valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
Intraday data cannot extend last 60 days

start: str Download start date string (YYYY-MM-DD) or _datetime, inclusive.
Default is 99 years ago
E.g. for start="2020-01-01", the first data point will be on "2020-01-01"

end: str Download end date string (YYYY-MM-DD) or _datetime, exclusive.
Default is now
'''
def read_Historical_Data_From_Yf_with_startdate_and_enddate(symbolName, startDateInYYYYMMDD, endDateInYYYYMMDD, intervalInString):
    try:
        stockTicker = yfinance.Ticker(symbolName)
        df= stockTicker.history(startDateInYYYYMMDD, endDateInYYYYMMDD, intervalInString)
        if df.empty:
            print("No data returned, possibly due to invalid parameters.")
        return df
    except Exception as e:
        print(f"Error fetching data: {e}")
        return None


'''
modify the Yahoo Finance Date Column Series into pandas datetype column Series of
datetime64[ns, UTC+05:30] having datetime and timezone info
'''
def convert_yf_datetime_to_pandas_datetime(dataframe):
    dataframe['Date'] = pd.to_datetime(dataframe['Date'])
    return dataframe

'''
Convert Pandas datetime64 series formatString into a user specified format(python datetime)
format e.g '%d-%m-%Y'
'''
def convert_datetime64_to_user_format(pandaSeries, formatString):
    return pandaSeries.dt.strftime(formatString)

'''
Filter pandas dataframe for the given column within the given range filter
[rangeStart, rangeEnd]
for e.g Filter pandas dataframe Datetime (in datetime64 ) format between 
given start_date and end_date ...Example below 
    rangeStart = '2024-03-07'
    rangeEnd = '2024-08-31'
'''
def filter_panda_df_in_range_constraint(dataframe, columnName, rangeStart, rangeEnd):
    return dataframe[(dataframe[columnName] >= rangeStart) & (dataframe[columnName] <= rangeEnd)].copy()

'''
Filter pandas dataframe wrt specifi column condition
'''
def filter_pands_df_column_filter(dataframe, columnName, filterConditionValue):
    return dataframe[dataframe[columnName] == filterConditionValue]

    """
    Filter the DataFrame based on a list of values for a specified column.
    
    Parameters:
    dataframe: pd.DataFrame - The DataFrame to filter.
    columnName: str - The column name to apply the filter on.
    filterConditionList: list - The list of values to filter by.
    
    Returns:
    pd.DataFrame - The filtered DataFrame.
    """
def filter_pands_df_column_filter_on_conditionlist(dataframe, columnName, filterConditionList):
    # Filter the DataFrame based on the condition list
    filtered_df = dataframe[dataframe[columnName].isin(filterConditionList)]
    return filtered_df



'''
Create a Pandas Series With Percentage Change for the given
input series with the given columnName having float/int values
columnToAdd -> Is the new column Name which has to added which stores output
columnName -> Is the input column in the dataframe against which pct_change is applied
Return panda dataframe
'''

'''
Create a Pandas Series With Diff Change for the given
input series with the given columnName having float/int values
columnToAdd -> Is the new column Name which has to added which stores output
columnName -> Is the input column in the dataframe against which diff() is applied
Return panda dataframe
'''
def generatePctChangeForGivenColumn(dataFrame, columnToAdd, columnName):
    dataFrame[columnToAdd] = dataFrame[columnName].pct_change() * 100
    return dataFrame

def generateDiffChangeFoGivenColumn(dataFrame, columnToAdd, columnName):
    dataFrame[columnToAdd] = dataFrame[columnName].diff()
    return dataFrame


'''
Remove TimeZone Info from the DateTime64 Series ColumnName
'''
def remove_timeZoneInfo_from_datetime(dataFrame, columnName):
    dataFrame[columnName]  = dataFrame[columnName].dt.tz_localize(None)
    return dataFrame

'''
Get the Year and Month from the panda Datetime Series Column
Example 2024-09-14 will result in 2024-08
'''
def convert_datetime_to_yearmonth(dataframe, columnName):
    return dataframe[columnName].dt.to_period('M').copy()

'''
Return the product of all elements in Series
'''
def get_product_of_series_date(dataframe, columnName):
    return dataframe[columnName].prod()


'''
Gives Size of dataframe
'''
def get_dataframe_size(dataframe):
    return dataframe.shape[0]

'''
Sets a given value for the dataframe for an index and column
rowNumber -> Given row index
columnName -> given column label
'''
def set_dataframe_value_for_index_and_columnName(dataframe, rowNumber, columnName, value):
    ind = dataframe.index[rowNumber]
    dataframe.loc[ind, columnName] = value
    return dataframe

'''
Sets a given value for the dataframe for conditional value of the given column
cond -> Given condition when its true
columnName -> given column label
'''
def set_dataframe_value_for_index_and_columnName_for_conditionalCheck(dataframe, cond, columnName, value):
    sz = get_dataframe_size(dataframe)
    for i in range(sz):
        index = dataframe.index[i]
        if dataframe.loc[index, columnName] == cond:
            dataframe.loc[index, columnName] = value
    
    return dataframe

'''
Remove column from dataframe
'''
def remove_dataframe_column(dataframe, columnName):
    dataframe = dataframe.drop(columns=[columnName])  # Drop the specified column
    return dataframe

'''
For a given dictionary return the dictionary in a sorted way
in descending order(reverse=True) and sorting in the value of the elements
'''
def return_sorted_dictionary_from_dict(dictionary, descending=True):
    return dict(sorted(dictionary.items(), reverse = descending, key=lambda item: item[1]))

'''
Create the 2 series dataframe from the given python dictionary
'''
def create_df_from_pydictionary(dictionary):
    columnList = list(dictionary.keys())
    return pd.DataFrame(list(dictionary.items()), columns=columnList)

'''
Merge Two dataframe based on certain column condition value
'''
def get_intersection_of_two_dataframe(df1, df2, columnName, condition):
    pd.merge(df1[df1[columnName] == condition], df2[df2[columnName] == condition], on=columnName)

'''
Get the dataframe with given integer index  values
'''
def get_dataframe_rows_with_index_range(df1, indexRange1, indexRange2):
    df1.iloc[indexRange1:indexRange2, :]

'''
Get the dataframe with given index label values
'''
def get_dataframe_rows_with_labelindex_range(df1, label1, label2):
    df1.loc[label1:label2, :]

'''
Filter dataframe with given column labels list
e.g     a	b
    0	12	34
    1	23	33

    df.loc[:, ['b']] will result in 
    	b
    0	34
    1	33
'''
def get_dataframe_with_columnlabel_list(df, columnList):
    df.loc[:, columnList]

def dropAllNan_Row_Entries_InDataframe(df):
    df.dropna()


def add_row_to_dataframe(df, row_values):
    """
    Adds a new row to an existing DataFrame.

    Parameters:
    df (pd.DataFrame): The DataFrame to which the row will be added.
    row_values (dict): A dictionary where keys are column names and values are the row values.

    Returns:
    pd.DataFrame: The DataFrame with the new row added.
    """
    # Convert the dictionary to a DataFrame and append it
    new_row = pd.DataFrame([row_values])
    
    # Append the new row to the existing DataFrame and reset the index
    df = pd.concat([df, new_row], ignore_index=True)
    
    return df

def find_current_directory():
    return os.path.dirname(os.path.abspath(__file__))

def createDirectory(pathDirectory):
    # Define the path to the new directory
    directory_path = pathDirectory
    # Create the directory
    try:
        os.makedirs(directory_path, exist_ok=True)
        print(f"Directory created: {directory_path}")
    except OSError as e:
        print(f"Error creating directory: {e}")

'''
Download the symbols from Yahoo and store in the given directory
with the specified format...Also Put the period and interval in the input
for the date range and intervals for which the data has to be downloaded
'''
def download_Mkt_Data_For_Symbols_And_SaveFilesToDirectory(symbolList, periodInString, intervalInString, dirname, format):
    for i in range (len(symbolList)):
        currentSymbol = symbolList[i] + ".NS"
        if os.path.exists(dirname + "/" + currentSymbol + format):
            return
        stockHistoricalDf = read_Historical_Data_From_Yf_with_period(symbolName=currentSymbol, periodInString=periodInString, intervalInString=intervalInString)
        if stockHistoricalDf.empty == False:
            save_df_to_csv(stockHistoricalDf, dirname, currentSymbol + format)
        else:
            print ("The symbol " + currentSymbol + " doesnt have historical data")

'''
Create a union using set
'''
def create_union_of_two_pylist(list1, list2):
    union_list = list(set(list1) | set(list2))
    return union_list
     
