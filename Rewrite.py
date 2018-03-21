'''
Going to rewrite the Program with the following Goals:
	- IS ENTIRELY DYNAMIC
		- Namebank, PUCount, Inserting New Entries, Handling Nulls in DB... Etc.
		- I have some ideas in ver 3 you should look at. It involves reading the CSV
			and pulling out names included for the name bank. This should let us
			Build the DB and report on it so long as we handle Nulls
	- IS CLEAN TO LOOK AT
		- I don't want any variables that are fucking Obscure (specialist[specialList[0]] - SRS?)
	- The same if not less lines of code than the original
		- Roughly 400 uncommented was ver2
	- Better SQL Code
		- Those got hefty. Maybe learn some more SQL iterate with an insert into instead of typing out specialist[1-13]
	- Written In one Week (Before my final QA run so I can give it to Natasha or whoever)
	- Have a nice end product to put on GitHub
'''

''' 
	*** NEXT STEPS ***
		- Create DB connection
		- Create Read file
		- Start testing QQ

'''
import sqlite3
import time
import datetime
import csv
from functools import reduce
import xlsxwriter

''' VARIABLES '''

date = time.strftime('%m-%d-%Y')
year = time.strftime('%Y')
Name_Bank = []
Pickup_Count = []
Assign_Count = []
Putup_Count = []
Total_QA = 0
Total_Assigned = 0
Total_UnAssigned = 0
Total_Initial = 0
Total_Delivery = 0
Total_Launch = 0
Total_SC = 0
Total_Payment = 0
Total_YRU = 0
Total_Other = 0
Time_Complete = []
Time_Due = []
Average_Complete_Time = []
Average_Due_Time = []

''' FUNCTIONS '''

	#PREPARATION FUNCTIONS
def create_list_data(*args):
	for i in args:
	    for j in i:
		    Pickup_Count.append(j)
		    Pickup_Count.append(0)
		    Assign_Count.append(j)
		    Assign_Count.append(0)
		    Putup_Count.append(j)
		    Putup_Count.append(0)

	putUp = c.execute("SELECT * FROM PutUpQA")
	Put_Headers = list(map(lambda x: x[0], c.description))
	pickup = c.execute("SELECT * FROM PickedUpQA")
	Pick_Headers = list(map(lambda x: x[0], c.description))
	assign = c.execute("SELECT * FROM AssignedQA")
	Assign_Headers = list(map(lambda x: x[0], c.description))

	for i in Pick_Headers:
		if i not in Pickup_Count:
	    	Pickup_Count.append(i)
	    	Pickup_Count.append(0)
	for i in Put_Headers:
		if i not in Putup_Count:
		    Putup_Count.append(i)
		    Putup_Count.append(0)
	for i in Assign_Headers:
		if i not in Assign_Count:
		    Assign_Count.append(i)
		    Assign_Count.append(0)

def create_tables():
	Table_Bank = ["AssignedQA","PickedUpQA","PutUpQA"]
	c.execute("CREATE TABLE IF NOT EXISTS 'Metrics'('Date' TEXT, TotalQA TEXT, AssignedQA TEXT, UnAssigned TEXT, AverageTimeUntilCompleted TEXT, AverageTimeUntilDueDate TEXT)")
	c.execute("CREATE TABLE IF NOT EXISTS 'Totals'('Date' TEXT, Initial TEXT, Delivery TEXT, Launch TEXT, SchoolChoice TEXT, Payment TEXT, YRU TEXT, Other TEXT)")
	#Ideally after the initial DB read AND export read so we can have a full name list of things we need to create
	#Maybe not super important, given this is the creation and not the insert actually.
	'''   This would be the replacement and would require the above changes
	Name_Str = "'Date' TEXT"
	for i in nameBank:
		hold = ", '" + i "' TEXT"
		Name_Str += hold
	for i in Table_Bank:
		c.execute("CREATE TABLE IF NOT EXISTS '" + i + "'('" + Name_Str + ")")		
	'''
	for i in Table_Bank:
		c.execute("CREATE TABLE IF NOT EXISTS '" + i + "'('Date' TEXT, 'Sarah Khan' TEXT, 'Patryk Szuszkiewicz' TEXT, 'Alex Oestreicher' TEXT, 'Nolan Kingdon' TEXT, 'Phil Hobrla' TEXT, 'Dale Fillpot' TEXT, 'Stratton Barry' TEXT, 'Rachel Shaw' TEXT,'Zachary Scott' TEXT, 'Duy Trinh' TEXT, 'Laura Jaczenko' TEXT, 'Diane Sellars' TEXT)")
	for i in Name_Bank:
		c.execute("CREATE TABLE IF NOT EXISTS '" + i + "'('Date' TEXT, 'Taken By' TEXT, DueDate TEXT, Netsuite TEXT, LeadSpecialist TEXT, District TEXT, Year TEXT, Solution TEXT, QAType TEXT, SIS TEXT, ENT TEXT, Payment TEXT, Localization TEXT, SC TEXT, SSO TEXT, SchoolLocator TEXT, NorthCarolina TEXT, EmailHistory TEXT, CompletedDate TEXT, Submitted TEXT, qa_Assigned TEXT)")

def append_table_cols(specialist):
	tables = ['AssignedQA','PickedUpQA','PutUpQA']
	for i in tables:
		c.execute("ALTER TABLE " + i + " ADD " + specialist + " TEXT")	

	#DATA ENTRY FUNCTIONS

def data_entry_Metrics(today, TotalQA, AssignedQA, UnAssigned, AverageTimeUntilCompleted, AverageTimeUntilDueDate):

    c.execute("INSERT INTO Metrics ('Date', TotalQA, AssignedQA, UnAssigned, AverageTimeUntilCompleted, AverageTimeUntilDueDate) VALUES (?,?,?,?,?,?)",
          (today, TotalQA, AssignedQA, UnAssigned, AverageTimeUntilCompleted, AverageTimeUntilDueDate))
    conn.commit()

def data_entry_Totals(today, Initial, Delivery, Launch, SchoolChoice, Payment, YRU, Other):
    
    c.execute("INSERT INTO Totals ('Date', Initial, Delivery, Launch, SchoolChoice, Payment, YRU, Other) VALUES (?,?,?,?,?,?,?,?)",
          (today, Initial, Delivery, Launch, SchoolChoice, Payment, YRU, Other))
    conn.commit()

def data_entry_AssignedQA(today, Assign_Count):	
	#This needs to happen AFTER the name cols in the DB have been updated	
	c.execute("SELECT * FROM AssignedQA")

	nameList = list(map(lambda x: x[0], c.description))

	Dynamic_Names = ""
	Dynamic_Values = ""
	List_Data = [today]

	for i in Assign_Count:
		if type(i) == int:
			List_Data.append(i)
	while len(nameList) > len(List_Data)-1:
		List_Data.append(0)

	Tuple_Data = tuple(List_Data)

	for i in nameList:
 		Dynamic_Names += ("'" + str(i) + "', ")
 		Dynamic_Values += "?, "

    c.execute("INSERT INTO AssignedQA('Date', " + Dynamic_Names[:-2] + " VALUES (" + Dynamic_Values[:-2] + ")", Tuple_Data)
    conn.commit()

def data_entry_PickedUpQA(today, Pickup_Count):
	#This needs to happen AFTER the name cols in the DB have been updated	
	c.execute("SELECT * FROM PickedUpQA")

	nameList = list(map(lambda x: x[0], c.description))

	Dynamic_Names = ""
	Dynamic_Values = ""
	List_Data = [today]

	for i in Pickup_Count:
		if type(i) == int:
			List_Data.append(i)
	while len(nameList) > len(List_Data)-1:
		List_Data.append(0)

	Tuple_Data = tuple(List_Data)

	for i in nameList:
 		Dynamic_Names += ("'" + str(i) + "', ")
 		Dynamic_Values += "?, "

    c.execute("INSERT INTO PickedUpQA('Date', " + Dynamic_Names[:-2] + " VALUES (" + Dynamic_Values[:-2] + ")", Tuple_Data)
    conn.commit()
    
def data_entry_PutUpQA(today, Putup_Count):
	#This needs to happen AFTER the name cols in the DB have been updated	
	c.execute("SELECT * FROM PutUpQA")

	nameList = list(map(lambda x: x[0], c.description))

	Dynamic_Names = ""
	Dynamic_Values = ""
	List_Data = [today]

	for i in Putup_Count:
		if type(i) == int:
			List_Data.append(i)
	while len(nameList) > len(List_Data)-1:
		List_Data.append(0)

	Tuple_Data = tuple(List_Data)

	for i in nameList:
 		Dynamic_Names += ("'" + str(i) + "', ")
 		Dynamic_Values += "?, "

    c.execute("INSERT INTO PutUpQA('Date', " + Dynamic_Names[:-2] + " VALUES (" + Dynamic_Values[:-2] + ")", Tuple_Data)
    conn.commit()
  
def data_entry_Totals(today, Initial, Delivery, Launch, SchoolChoice, Payment, YRU, Other):
    
    c.execute("INSERT INTO Totals ('Date', Initial, Delivery, Launch, SchoolChoice, Payment, YRU, Other) VALUES (?,?,?,?,?,?,?,?)",
          (today, Initial, Delivery, Launch, SchoolChoice, Payment, YRU, Other))
    conn.commit()

def data_entry_Specialists(today, csv_File):
	#This is probably fine TBH
	for row in csv_File[1:]:
		c.execute('INSERT INTO "' + row[3] + '"("Date", "Taken By", "DueDate", Netsuite, LeadSpecialist, District, Year, Solution, QAType, SIS, ENT, Payment, Localization, SC, SSO, SchoolLocator, NorthCarolina, EmailHistory, CompletedDate, Submitted, qa_Assigned) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
		(today, row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13], row[14], row[15], row[16], row[17], row[18], row[19]))
	conn.commit()

''' CSV READ '''

def metrics(readCSV, nameBank):
	#Writing everything once as a global was faster than writing everything twice as paramaters/Arguments.
	global Pickup_Count
	global Assign_Count
	global Putup_Count
	global Total_QA
	global Total_Assigned
	global Total_UnAssigned
	global Total_Initial
	global Total_Delivery
	global Total_Launch
	global Total_SC
	global Total_Payment
	global Total_YRU
	global Total_Other
	global Time_Complete
	global Time_Due
	global Average_Complete_Time
	global Average_Due_Time

	#Counting Unassigned QAs
	for num in readCSV:
		if num[0] == "":
			Total_UnAssigned += 1
	iter(readCSV)
	#Counting number of QAs
	for i in readCSV[1:]:
		Total_QA += 1
	iter(readCSV)
	#Indexing Amounts of QA types
	for k in readCSV:
		if k[7] == 'Initial Solution':
			Total_Initial += 1;
	iter(readCSV)
	for l in readCSV:
		if l[7] == 'Delivery':
			Total_Delivery += 1;
	iter(readCSV)
	for b in readCSV:
		if b[7] == 'Payment':
			Total_Payment += 1;
	iter(readCSV)
	for sc in readCSV:
		if sc[7] == 'School Choice Form' or sc[7] == 'School Choice Lottery':
			Total_SC += 1
	iter(readCSV)
	for j in readCSV:
		if j[7] == 'Launch':
			Total_Launch +=1
	iter(readCSV)
	for y in readCSV:
		if y[7] == 'YRU':
			Total_YRU += 1
	iter(readCSV)
	for o in readCSV:
		if o[7] == 'Other':
			Total_Other += 1
	iter(readCSV)
	#Tracking Picked up QAs
	for assigned in readCSV[1:]:
		if assigned[0] != "":
			Total_Assigned +=1
	iter(readCSV)
	
	#Tracking who picked up QAs (I messed this up so it's inverted. Was easier to fix the label)
	for ass in readCSV:
		if ass[0] in nameBank and ass[-1] =="1":
			ACount[ass[0]] += 1
	#Tracking who was assigned QAs (ibid)
	for row in readCSV:
		if row[0] in nameBank and row[-1] == "0":
			PUCount[row[0]] += 1
	#Tracking who put in QAs
	for row in readCSV:
		if row[3] in nameBank:
			PutCount[row[3]] += 1

	iter(readCSV)
	#Dates
	for i in readCSV[1:]:
		timeUntilDue(i[-2],i[1])
	amtOfDueDates = len(Time_Due)
	Average_Due_Time = round((reduce((lambda x,y: x+y), Time_Due))/amtOfDueDates, 2)
	iter(readCSV)

	for i in readCSV[1:]:
		timeUntilComplete(i[-2],i[-3])
	amtOfCompletes = len(Time_Complete)
	Average_Complete_Time = round((reduce((lambda x,y: x+y), Time_Complete))/amtOfCompletes, 2)
	iter(readCSV)

	for i in readCSV[1:]:
		if i not in nameBank:
			nameBank.append(i)
	return nameBank
	
''' SQL WRITE '''


''' SQL READ '''

def read_from_db_AssPickPut(table):
    c.execute('SELECT * FROM ' + table + '')
    data = c.fetchall()
    listData = list(data)
    return listData

def read_from_db_Metrics():
	c.execute('SELECT * FROM Metrics')
	data = c.fetchall()
	listData = list(data)
	return listData

def read_from_db_Totals():
	c.execute('SELECT * FROM Totals')
	data = c.fetchall()
	listData = list(data)
	return listData

''' XLSX WRITE '''
