# Emma McNeill
# The purpose of this program is to import 3 files with housing data, clean
#   the data, and load the cleaned data into sql database


from files import *
import pymysql
import numpy as np
import pandas as pd
import re
from creds import *

#Import files
housingInfoFile = pd.read_csv(housingFile)
incomeInfoFile = pd.read_csv(incomeFile)
zipInfoFile = pd.read_csv(zipFile)

housingAndInfoFile = housingInfoFile.join(incomeInfoFile.set_index('guid'), on='guid', lsuffix='_housing', rsuffix='_income')
allFiles = housingAndInfoFile.join(zipInfoFile.set_index('guid'), on='guid')
allFiles = allFiles.drop(columns=['zip_code_housing', 'zip_code_income'])
allFiles = allFiles.rename(columns={'housing_median_age': 'median_age'})

listOfAll = []

for guid, row in allFiles.iterrows():
    dehyphen = row['guid'].replace('-', "")
    listOfAll.append(dehyphen)

guidData = pd.DataFrame(data=listOfAll, columns=['guid'])
allFiles.update(guidData)

#Cleaning data

#   Drop guid entry
for guid, row in allFiles.iterrows():
    correct = re.fullmatch("[A-Za-z][A-Za-z][A-Za-z][A-Za-z]", row['guid'])

    if correct is not None:
        badGuid = row['guid']
        allFiles.replace(badGuid, np.nan, True)
        allFiles.dropna(inplace=True)
    else:
        pass

#   Check for good zipcode
for code, row in allFiles.iterrows():
    correct = re.search("[A-Za-z][A-Za-z][A-Za-z][A-Za-z]", row['zip_code'])

    if correct is not None:
        corruptGuid = row['guid']
        badZip = row['zip_code']
        city = row['city']
        state = row['state']

#   assign new zip to bad zipcode data
        for location, record in allFiles.iterrows():
            correctInfo = re.search("[A-Za-z][A-Za-z][A-Za-z][A-Za-z]", record['zip_code'])
            if correctInfo is None:
                if corruptGuid != record['guid']:
                    if state == record['state']:
                        zipcode = record['zip_code']
                        result = [int(x) for x in str(zipcode)]
                        first = result[0]
                        newZip = str(first) + "0000"
                        int(newZip)
                    else:
                        pass
                else:
                    pass

        allFiles.replace(badZip, newZip, True)
    else:
        pass

#   Check for good median age
for age, row in allFiles.iterrows():
    correct = re.search("[A-Za-z][A-Za-z][A-Za-z][A-Za-z]", row['median_age'])

#       Assign random value between 10 and 50 to bad data
    if correct is not None:
        badAge = row['median_age']
        randomAge = np.random.randint(10, 50)
        allFiles.replace(badAge, randomAge, True)
    else:
        pass

#   Check for good room data
for rooms, row in allFiles.iterrows():
    correct = re.search("[A-Za-z][A-Za-z][A-Za-z][A-Za-z]", row['total_rooms'])

#       Assign random value between 1000 and 2000 to bad data
    if correct is not None:
        badRooms = row['total_rooms']
        randomRoom = np.random.randint(1000, 2000)
        allFiles.replace(badRooms, randomRoom, True)
    else:
        pass

#   Check for good bedroom data
for bedrooms, row in allFiles.iterrows():
    correct = re.search("[A-Za-z][A-Za-z][A-Za-z][A-Za-z]", row['total_bedrooms'])

#       Assign random value between 1000 and 2000 to bad data
    if correct is not None:
        badBedroom = row['total_bedrooms']
        randomBedroom = np.random.randint(1000, 2000)
        allFiles.replace(badBedroom, randomBedroom, True)
    else:
        pass

#   Check for good population data
for population, row in allFiles.iterrows():
    correct = re.search("[A-Za-z][A-Za-z][A-Za-z][A-Za-z]", row['population'])

#       Assign random value between 5000 and 10000 to bad data
    if correct is not None:
        badPop = row['population']
        randomPop = np.random.randint(5000, 10000)
        allFiles.replace(badPop, randomPop, True)
    else:
        pass

#   Check for good household data
for households, row in allFiles.iterrows():
    correct = re.search("[A-Za-z][A-Za-z][A-Za-z][A-Za-z]", row['households'])

#       Assign random value between 500 and 2500 to bad data
    if correct is not None:
        badHHold = row['households']
        randomHHold = np.random.randint(500, 2500)
        allFiles.replace(badHHold, randomHHold, True)
    else:
        pass

#   Check for good median house value
for value, row in allFiles.iterrows():
    correct = re.search("[A-Za-z][A-Za-z][A-Za-z][A-Za-z]", row['median_house_value'])

#       Assign random value between 100000 and 250000 to bad data
    if correct is not None:
        badMHValue = row['median_house_value']
        randomMHValue = np.random.randint(100000, 250000)
        allFiles.replace(badMHValue, randomMHValue, True)
    else:
        pass

#   Check for good income value
for income, row in allFiles.iterrows():
    correct = re.search("^[A-Z]{4}$", row['median_income'])

#       Assign random value between 100000 and 750000 to bad data
    if correct is not None:
        badIncome = row['median_income']
        randomIncome = np.random.randint(100000, 750000)
        allFiles.replace(badIncome, randomIncome, True)
    else:
        pass

#Connect to sql
try:
    myConnection = pymysql.connect(host=hostname,
                                   user=username,
                                   password=password,
                                   db=database,
                                   charset='utf8mb4',
                                   cursorclass=pymysql.cursors.DictCursor)
except Exception as e:
    print(f"An error has occurred.  Exiting: {e}")
    print()
    exit()

# Upload records to database
cursor = myConnection.cursor()
column = "`,`".join([str(i) for i in allFiles.columns.tolist()])


for i, row in allFiles.iterrows():
    sql = "INSERT INTO `housing` (`" + column + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
    cursor.execute(sql, tuple(row))
    myConnection.commit()

# Import the merged files in SQL
sqlCount = "SELECT COUNT(*) FROM `housing`"
cursor.execute(sqlCount)
count = cursor.fetchall()
printRecords = len(count)
for result in count:
    totalRecords = result['COUNT(*)']

#Output
print("Beginning import")
print("Cleaning Housing File data")
print(f"{totalRecords} records imported into the database")
print("Cleaning Income File data")
print(f"{totalRecords} records imported into the database")
print("Cleaning ZIP File data")
print(f"{totalRecords} records imported into the database")
print("Import completed")
print()
print("Beginning validation")
print()

Validation = False
while Validation is False:
    inputRooms = input("Total Rooms:  ")
    try:
        if str.isnumeric(inputRooms):
            if int(inputRooms) >= 0:
                dataBedrooms = """select sum(total_rooms) from housing where total_rooms > %s"""
                cursor.execute(dataBedrooms, inputRooms)
                roomsResult = cursor.fetchall()
                for result in roomsResult:
                    totalBedrooms = roomsResult[0]['sum(total_rooms)']
                print(f"Total Rooms: {inputRooms}")
                print(f"For locations with more than {inputRooms} rooms, there are a total of {totalBedrooms} bedrooms.")
                print()
                Validation = True
        elif int(inputRooms) < 0:
            print("Please enter only positive integers")
            Validation = False
        else:
            print("Only integers.")
            Validation = False
    except Exception as e:
        print(f"Please try again.")
        print(f"\t{e}")

ValidationTwo = False
while ValidationTwo is False:
    inputZip = input("ZIP Code:  ")
    try:
        if str.isnumeric(inputZip):
            if int(inputZip) >= 0:
                if len(inputZip) == 5:
                    dataZip = "SELECT FORMAT(ROUND(AVG(median_income)),0) avg_income FROM housing WHERE zip_code = %s"
                    cursor.execute(dataZip, inputZip)
                    zipResult = cursor.fetchall()
                    for result in zipResult:
                        income = result['avg_income']
                    print(f"The median household income for ZIP code {inputZip} is {income}.")
                    print()
                    ValidationTwo = True
                else:
                    print("Please enter a 5 digit zipcode.")
                    ValidationTwo = False
        elif int(inputZip) < 0:
            print("Please don't be negative.")
            ValidationTwo = False
        else:
            print("Please only enter whole numbers.")
            ValidationTwo = False
    except Exception as e:
        print(f"Please try again.")
        print(f"\t{e}")

myConnection.close()

print("Program exiting.")
