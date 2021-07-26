from datetime import date, datetime
startTime = datetime.now()
#MEASURING TIME of the Script with datetime

import yfinance as yf # API connection
import pandas as pd # data processing 

from pathlib import Path # This objects let our paths to be readed both by windows and Unix systems

################# CONNECTION TO DATA AND API ##########################
#DATA from the Part 1 of the exercise
data_folder = Path("OUTPUT DATA/PART1/")
file_to_open = data_folder / "output1.csv"
input_data = pd.read_csv(file_to_open)
#Formating the Date field in our Data
input_data['Date'] = pd.to_datetime(input_data['Date'], format='%Y-%m-%d', errors='coerce')
#Changing the NaT (null date values) into the required datetime mentioned in
#the exercise: "2021-01-01"
input_data['Date'].replace({pd.NaT: pd.to_datetime("2021-01-01",format='%Y-%m-%d')}, inplace=True)
#Now, in order to connecto the YAHOO API we need to retrieve:
#- Date of today
#- Ticker name from the Data from the former exercise
#- first date we would like to retrieve data from.
#However, testing in jupyter notebooks seems more efficient to 
#just download all the data at once. Therefore, we will state 
#the objective minimun date "2021-01-01". I am appending commented 
#code for connecting to the API retrieving data from different dates
#at the end of the srcript. :) 

#Date of Today
today = str(date.today())
#retrieving a string separated by space between our tickers of interest
name_list= input_data.Ticker.unique()
name_list_to_finance = ["".join(item) for item in name_list.astype(str)]
string = ' '.join(name_list_to_finance)
#Connecting to the API and storing the dataframe in "datas"
datas = yf.download(string, start="2021-01-01", end=today)

### RETRIEVING DATA
#Initialise the variable of interest
missing_Data_Output = pd.DataFrame([])

#now we search only for the information of interest in order to be as efficient as possible
#How it is done?

#1 First we read every row from the data in the exercise before
#2 Then based on this data we make a "query" in the large Dataset dowloaded
#extracting a "block" of information with the values we want to extract
#3 Finally we create a dataset that would be ultimately concatenated 
#with the rest retrieved in this loop through our initial dataset

#In this way, by retrieving "blocks" of information and creating new ones, we avoid 
#inefficient looping and appending.
#Loop based on the data from the last exercise starts.
for x in range(len(input_data)):
    time = input_data["Date"][x] #we extract the date of the row
    filter_time = datas.index >= time #prepare the filter for the query in the large dataset "datas"
    ticker = input_data["Ticker"][x] #extract the name of the row
    field = input_data["Field Name"][x] #Extract the name of the ticker of the row
    value = datas[filter_time]["Close",ticker].dropna()#retrieval of a pd series OBJECT with:the values of interest "Close" / the date as a index / droping "Nan" values 
    list_of_field = [field for i in range(len(value))] #creation of list with the name of the field and the ticker to avoid unnecessary looping
    list_of_ticker = [ticker for i in range(len(value))]
    list_of_list = [list(value.index),list_of_ticker,list_of_field,list(value)]
    df = pd.concat([pd.Series(x) for x in list_of_list], axis=1) #Creating the DataFrame object by concatenating the 4 lists in "list of list"
    df.columns = ["Date", "Ticker", "Field Name", "Value"] #assigning the columns title
    if time != "2021-01-01":
        df = df.iloc[1: , :] #eliminating the first row of the pd if the date is different from the asked one
    missing_Data_Output = pd.concat([missing_Data_Output, df])

################# SAVING THE MISSING DATA ##########################
data_folder = Path("OUTPUT DATA/PART2/")
file_to_save = data_folder / "output2.csv"
missing_Data_Output.to_csv(file_to_save, index = False)
print(missing_Data_Output)

#Retrieving time 
endTime= datetime.now()
executionTime = ( endTime - startTime)
print('Execution time in seconds: ' + str(executionTime))

#for name in name_list:
#   name_filter = input_data["Ticker"] == str(name)
#   minimun_date = input_data[name_filter]["Date"].min()
#   data = yf.download(name, start=minimun_date, end=today)
#   data_array.append({name:data})
#   key_array.append(name)
