import time
startTime = time.time()
import pandas as pd
import numpy as np #linear algebra / arrays

from pathlib import Path
from os import listdir

#Function for retrieving the names of the csv files in a certain folder
def find_csv_filenames(path_to_dir, suffix=".csv" ):
    filenames = listdir(path_to_dir)
    return [ filename for filename in filenames if filename.endswith( suffix ) ]

# Function for assigning the values when there is no data of one of the fields
# to be improved with .unique() for being more dinamic. 
def CheckIntegrity(row):
    if row["Field Name"] == "Close Price":
        value = "Volume"
        return value
    elif row["Field Name"] == "Volume":
        value = "Close Price"
        return value

# Function for retrieving the last date and value asssigned to it depending on the value of the field Name
def SearchMissingValue(data,value):
    FieldName_Filter = data["Field Name"] == value.values[0]
    Date_Filter = data[FieldName_Filter]["Date"].max()
    FoundValue = data[np.logical_and(FieldName_Filter, data["Date"] == Date_Filter)]["Value"]
    return Date_Filter,FoundValue

#Arrays for storing the key and DataFrames later in a dictionary
data_array= []
key_array= []

#OPENING THE DATA PROVIDED
#Opening the reference table
PATH_INSTRUMENTS = Path('INPUT DATA/REFERENTIAL/INSTRUMENTS.csv')
instruments_raw = pd.read_csv(PATH_INSTRUMENTS, sep= ";")
#And the ticker values of all the intruments that we need to find values for
reference_intruments_name = instruments_raw["Ticker"].values

#Opening all files in DB folder
GENERAL_PATH = Path("INPUT DATA/DB/")
filenames = find_csv_filenames(GENERAL_PATH)

#Opening the data provided and appending empty DataFrames when the reference instruments are not found
for name in reference_intruments_name:
    name_csv = name + '.csv' 
    if name_csv in filenames:
        data = pd.read_csv(GENERAL_PATH / name_csv, sep= ";")
        data = data.loc[:,~data.columns.str.match("Unnamed")]
        data['Date'] = pd.to_datetime(data['Date'], format='%d-%m-%y', errors='raise')
        data_array.append({name:data})
        key_array.append(name)
    else:
        data_array.append({name:pd.DataFrame()})
        key_array.append(name)

#Inisializating the DataFrame to be extracted
FinalData = pd.DataFrame([])
#Creating an array for storing the keys not found in our DB
emptyKeys = []

#Loop for creating the dataframe by reading the dictionary we have created
for index in range(len(key_array)):
    data = data_array[index][key_array[index]]
    if data.empty:
        emptyKeys.append(key_array[index])
        pass
    else:
        lastDate = data["Date"] == data["Date"].max()
        updatedData = data[lastDate]
        if len(updatedData["Field Name"]) == 1:
            valueName = updatedData.apply(CheckIntegrity, axis=1)
            newLastdate, newValue = SearchMissingValue(data,valueName)
            if newValue.empty:
                newData = {
                data.columns[0]:updatedData["Ticker"].values[0],
                data.columns[1]:newLastdate,
                data.columns[2]:valueName.values[0],
                data.columns[3]:None
                }
                df1 = pd.DataFrame(newData, index =[1])
            else:
                newData = {
                    data.columns[0]:updatedData["Ticker"].values,
                    data.columns[1]:newLastdate,
                    data.columns[2]:valueName.values,
                    data.columns[3]:newValue.values
                }
                df1 = pd.DataFrame(newData, index =[newValue.index[0]])    
            updatedData = pd.concat([updatedData, df1])
    
        FinalData = pd.concat([FinalData,updatedData])

#appending the structure for the empty keys
for x in emptyKeys:
    for y in FinalData["Field Name"].unique():
        FinalData.loc[len(FinalData)] = [x, None , y , None]

#SAving the data in the file "output1.csv"
data_folder = Path("OUTPUT DATA/PART1/")
file_to_save = data_folder / "output1.csv"
FinalData.to_csv(file_to_save, index = False)
print(FinalData)

executionTime = (time.time() - startTime)
print('Execution time in seconds: ' + str(executionTime))