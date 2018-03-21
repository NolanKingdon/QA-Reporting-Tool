import sqlite3
import time
import datetime
import csv
from functools import reduce
import xlsxwriter


#Dynamic Time - String
date = time.strftime('%m-%d-%Y')
year = time.strftime('%Y')
nameBank = [0, 'S', 'P', 'A', 'Nolan Kingdon', 'Ph', 'D', 'St', 'R','Z', 'D', 'L', 'Di']

#Picked up QAs
PUCount = {"D":0,"Nolan Kingdon":0,"P":0,"R":0,"Ph":0,"Z":0,"A":0,"D":0,"S":0,"St":0, "L":0, "Di":0}
ACount = {"D":0,"Nolan Kingdon":0,"P":0,"R":0,"Ph":0,"Z":0,"A":0,"D":0,"S":0,"St":0, "L":0, "Di":0}
PutCount = {"D":0,"Nolan Kingdon":0,"P":0,"R":0,"Ph":0,"Z":0,"A":0,"D":0,"S":0,"St":0, "L":0, "Di":0}
#QA Number vars
totalQA = 0
assignedQA = 0
unAss = 0
#QA Type vars
totalInitial = 0
totalDelivery = 0
totalLaunch = 0
totalSC = 0
totalPayment = 0
totalYRU = 0
totalOther = 0
StCHold = []
StDHold = []
avgTUC = 0
avgTUD = 0

def create_tables():
	# NULL, INTEGER, REAL, TEXT, BLOB
	c.execute("CREATE TABLE IF NOT EXISTS 'Metrics'('Date' TEXT, TotalQA TEXT, AssignedQA TEXT, UnAssigned TEXT, AverageTimeUntilCompleted TEXT, AverageTimeUntilDueDate TEXT)")
	c.execute("CREATE TABLE IF NOT EXISTS 'Totals'('Date' TEXT, Initial TEXT, Delivery TEXT, Launch TEXT, SchoolChoice TEXT, Payment TEXT, YRU TEXT, Other TEXT)")
	c.execute("CREATE TABLE IF NOT EXISTS 'AssignedQA'('Date' TEXT, 'S' TEXT, 'P' TEXT, 'A' TEXT, 'Nolan Kingdon' TEXT, 'Ph' TEXT, 'D' TEXT, 'St' TEXT, 'R' TEXT,'Z' TEXT, 'D' TEXT, 'L' TEXT, 'Di' TEXT)")
	c.execute("CREATE TABLE IF NOT EXISTS 'PickedUpQA'('Date' TEXT, 'S' TEXT, 'P' TEXT,'A' TEXT, 'Nolan Kingdon' TEXT,'Ph' TEXT, 'D' TEXT,'St' TEXT, 'R' TEXT,'Z' TEXT, 'D' TEXT, 'L' TEXT, 'Di' TEXT)")
	c.execute("CREATE TABLE IF NOT EXISTS 'PutUpQA'('Date' TEXT, 'S' TEXT, 'P' TEXT,'A' TEXT, 'Nolan Kingdon' TEXT,'Ph' TEXT, 'D' TEXT,'St' TEXT, 'R' TEXT,'Z' TEXT, 'D' TEXT, 'L' TEXT, 'Di' TEXT)")
	#Specialist Tables - Holds all info reported pertaining to the specialist (For future use if necessary)
	for i in nameBank[1:]:
		c.execute("CREATE TABLE IF NOT EXISTS '" + i + "'('Date' TEXT, 'Taken By' TEXT, DueDate TEXT, Netsuite TEXT, LeadSpecialist TEXT, District TEXT, Year TEXT, Solution TEXT, QAType TEXT, SIS TEXT, ENT TEXT, Payment TEXT, Localization TEXT, SC TEXT, SSO TEXT, SchoolLocator TEXT, NorthCarolina TEXT, EmailHistory TEXT, CompletedDate TEXT, Submitted TEXT, qa_Assigned TEXT)")

#SQLITE ENTRY FUNCTIONS
def data_entry_Metrics(today, TotalQA, AssignedQA, UnAssigned, AverageTimeUntilCompleted, AverageTimeUntilDueDate):

    c.execute("INSERT INTO Metrics ('Date', TotalQA, AssignedQA, UnAssigned, AverageTimeUntilCompleted, AverageTimeUntilDueDate) VALUES (?,?,?,?,?,?)",
          (today, TotalQA, AssignedQA, UnAssigned, AverageTimeUntilCompleted, AverageTimeUntilDueDate))
    conn.commit()

def data_entry_Totals(today, Initial, Delivery, Launch, SchoolChoice, Payment, YRU, Other):
    
    c.execute("INSERT INTO Totals ('Date', Initial, Delivery, Launch, SchoolChoice, Payment, YRU, Other) VALUES (?,?,?,?,?,?,?,?)",
          (today, Initial, Delivery, Launch, SchoolChoice, Payment, YRU, Other))
    conn.commit()

def data_entry_AssignedQA(today, specialist):
    #If I write the INSERT function to be a bit longer, I could probably get this all on one georgeous line in the DB:
    #c.execute("INSERT INTO AssignedQA('Date', '" + ACount[0] + "', '" ACount[1] "') VALUES (?,?,?)", (today, ACount[Acount[0]]... etc)
    specialList = list(specialist)

    c.execute("INSERT INTO AssignedQA('Date', " + "'" + specialList[0] + "'" + "," + "'" + specialList[1] + "'" + "," + "'" + specialList[2] + "'" + "," + "'" + specialList[3] + "'" + "," + "'" + specialList[4] + "'" + "," + "'" + specialList[5] + "'" + "," + "'" + specialList[6] + "'" + "," + "'" + specialList[7] + "'" + "," + "'" + specialList[8] + "'" + "," + "'" + specialList[9] + "'" + "," + "'" + specialList[10] + "'" + "," + "'" + specialList[11] + "'" + ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
          (today, specialist[specialList[0]],specialist[specialList[1]],specialist[specialList[2]],specialist[specialList[3]],specialist[specialList[4]],specialist[specialList[5]],specialist[specialList[6]],specialist[specialList[7]],specialist[specialList[8]],specialist[specialList[9]],specialist[specialList[10]],specialist[specialList[11]]))
    conn.commit()

def data_entry_PickedUpQA(today, specialist):
    
    specialList = list(specialist)

    c.execute("INSERT INTO PickedUpQA('Date', " + "'" + specialList[0] + "'" + "," + "'" + specialList[1] + "'" + "," + "'" + specialList[2] + "'" + "," + "'" + specialList[3] + "'" + "," + "'" + specialList[4] + "'" + "," + "'" + specialList[5] + "'" + "," + "'" + specialList[6] + "'" + "," + "'" + specialList[7] + "'" + "," + "'" + specialList[8] + "'" + "," + "'" + specialList[9] + "'" + "," + "'" + specialList[10] + "'" + "," + "'" + specialList[11] + "'" + ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
          (today, specialist[specialList[0]],specialist[specialList[1]],specialist[specialList[2]],specialist[specialList[3]],specialist[specialList[4]],specialist[specialList[5]],specialist[specialList[6]],specialist[specialList[7]],specialist[specialList[8]],specialist[specialList[9]],specialist[specialList[10]],specialist[specialList[11]]))
    conn.commit()

def data_entry_PutUpQA(today, specialist):
    
    specialList = list(specialist)

    c.execute("INSERT INTO PutUpQA('Date', " + "'" + specialList[0] + "'" + "," + "'" + specialList[1] + "'" + "," + "'" + specialList[2] + "'" + "," + "'" + specialList[3] + "'" + "," + "'" + specialList[4] + "'" + "," + "'" + specialList[5] + "'" + "," + "'" + specialList[6] + "'" + "," + "'" + specialList[7] + "'" + "," + "'" + specialList[8] + "'" + "," + "'" + specialList[9] + "'" + "," + "'" + specialList[10] + "'" + "," + "'" + specialList[11] + "'" + ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
          (today, specialist[specialList[0]],specialist[specialList[1]],specialist[specialList[2]],specialist[specialList[3]],specialist[specialList[4]],specialist[specialList[5]],specialist[specialList[6]],specialist[specialList[7]],specialist[specialList[8]],specialist[specialList[9]],specialist[specialList[10]],specialist[specialList[11]]))
    conn.commit()

def data_entry_Totals(today, Initial, Delivery, Launch, SchoolChoice, Payment, YRU, Other):
    
    c.execute("INSERT INTO Totals ('Date', Initial, Delivery, Launch, SchoolChoice, Payment, YRU, Other) VALUES (?,?,?,?,?,?,?,?)",
          (today, Initial, Delivery, Launch, SchoolChoice, Payment, YRU, Other))
    conn.commit()

def data_entry_Specialists(today, csv_File):
	#These are weird names for the for loops, but I had already went ahead and made the excel concat function for the row indices... sooo....
	for row in csv_File[1:]:
		c.execute('INSERT INTO "' + row[3] + '"("Date", "Taken By", "DueDate", Netsuite, LeadSpecialist, District, Year, Solution, QAType, SIS, ENT, Payment, Localization, SC, SSO, SchoolLocator, NorthCarolina, EmailHistory, CompletedDate, Submitted, qa_Assigned) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
		(today, row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13], row[14], row[15], row[16], row[17], row[18], row[19]))
	conn.commit()

#SQLITE READ FUNCTIONS

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

#CSV FUNCTIONS (Make metrics less ugly plz)
def metrics(readCSV):

	#Writing everything once as a global was faster than writing everything twice as paramaters/Arguments.
	global PUCount
	global ACount
	global PutCount
	global totalQA
	global assignedQA
	global unAss
	global totalInitial
	global totalDelivery
	global totalLaunch
	global totalSC
	global totalPayment
	global totalYRU
	global totalOther
	global StCHold
	global StDHold
	global avgTUC
	global avgTUD

	#FIGURE OUT A WAY TO MAKE THIS A LESS UGLY FUNCTION

	#Counting Unassigned QAs
	for num in readCSV:
		if num[0] == "":
			unAss += 1
	iter(readCSV)
	#Counting number of QAs
	for i in readCSV[1:]:
		totalQA += 1
	iter(readCSV)
	#Indexing Amounts of QA types
	for k in readCSV:
		if k[7] == 'Initial Solution':
			totalInitial += 1;
	iter(readCSV)
	for l in readCSV:
		if l[7] == 'Delivery':
			totalDelivery += 1;
	iter(readCSV)
	for b in readCSV:
		if b[7] == 'Payment':
			totalPayment += 1;
	iter(readCSV)
	for sc in readCSV:
		if sc[7] == 'School Choice Form' or sc[7] == 'School Choice Lottery':
			totalSC += 1
	iter(readCSV)
	for j in readCSV:
		if j[7] == 'Launch':
			totalLaunch +=1
	iter(readCSV)
	for y in readCSV:
		if y[7] == 'YRU':
			totalYRU += 1
	iter(readCSV)
	for o in readCSV:
		if o[7] == 'Other':
			totalOther += 1
	iter(readCSV)
	#Tracking Picked up QAs
	for assigned in readCSV[1:]:
		if assigned[0] != "":
			assignedQA +=1
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
	amtOfDueDates = len(StDHold)
	avgTUD = round((reduce((lambda x,y: x+y), StDHold))/amtOfDueDates, 2)
	
	iter(readCSV)

	for i in readCSV[1:]:
		timeUntilComplete(i[-2],i[-3])
	amtOfCompletes = len(StCHold)
	avgTUC = round((reduce((lambda x,y: x+y), StCHold))/amtOfCompletes, 2)
	iter(readCSV)

#Length of time a QA sits in the list before being complete
def timeUntilComplete(inTime, completeTime):

	global StCHold

	if completeTime == "":
		pass
	else:
		#To check to see if we add 31 or 30, or 28
		thirtyMonths = [1, 3, 5, 7, 8, 10, 12]

		splitIn = inTime.split("/")
		iD = int(splitIn[1])
		iM = int(splitIn[0])
		splitOut = completeTime.split("/")
		oD = int(splitOut[1])
		difDate = oD - iD
		if difDate < 0 and (iM in thirtyMonths and iM != 2):
			correctDate = 31 + difDate
			StCHold.append(correctDate)
		elif difDate < 0 and (iM not in thirtyMonths and iM != 2):
			correctDate = 30 + difDate
			StCHold.append(correctDate)
		elif difDate < 0 and iM == 2:
			correctDate = 28 + difDate
			StCHold.append(correctDate)
		else:
			StCHold.append(difDate)
#Function for Closeness to Deadline on submission
def timeUntilDue(inTime, outTime):

	global StDHold
	#To check to see if we add 31 or 30, or 28
	thirtyMonths = [1, 3, 5, 7, 8, 10, 12]

	splitIn = inTime.split("/")
	iD = int(splitIn[1])
	iM = int(splitIn[0])
	splitOut = outTime.split("/")
	oD = int(splitOut[1])
	difDate = oD - iD
	if difDate < 0 and (iM in thirtyMonths and iM != 2):
		correctDate = 31 + difDate
		StDHold.append(correctDate)
	elif difDate < 0 and (iM not in thirtyMonths and iM != 2):
		correctDate = 30 + difDate
		StDHold.append(correctDate)
	elif difDate < 0 and iM == 2:
		correctDate = 28 + difDate
		StDHold.append(correctDate)
	else:
		StDHold.append(difDate)
	#Run a check to see if date is <2 then note the specialist name.

#FUNCTION TO WRITE THE ENTIRE THING -- This is where we could optimize a bit with for loops probably

def write_report(date, year, updated_Metrics, updated_Totals, updated_PutUp, updated_PickedUp, updated_Assigned, allData, totals, workbook,header_format, title_format):
	totals.write(0, 2, "QA REPORT " + date, title_format)


	totals.write(4, 0, "Total QAs", header_format)
	totals.write(5, 0, "Taken/Assigned QAs", header_format)
	totals.write(6, 0, "Pending QAs", header_format)
	totals.write(7, 0, "Average Time Until Completed", header_format)
	totals.write(8, 0, "Average Time Until Due Date", header_format)

	#Writing the metrics - Move to a function that takes in year and updated_Metrics. Merge with above for a more clean look
	HeaderCount = 1
	RowCount = 4
	Header_List = [{"header":'Metric'}]
	for i in updated_Metrics:
		for j in i:
			if year in j:
				#Take this header writer, append it to a list instead, annnnnd.....
				totals.write(3, HeaderCount, j, header_format)
				HeaderCount += 1
				RowCount = 4
				Header_List.append({"header":j})
			if year not in j:
				totals.write(RowCount, HeaderCount-1, j)
				RowCount += 1
				#add it in as a part of the table so the columns will be from the list of dates.
	totals.add_table(3,0, RowCount-1, HeaderCount-1, {'columns':Header_List})


	totals.write(11, 0, "Initial", header_format)
	totals.write(12, 0, "Delivery", header_format)
	totals.write(13, 0, "Launch", header_format)
	totals.write(14, 0, "School Choice", header_format)
	totals.write(15, 0, "Payment", header_format)
	totals.write(16, 0, "YRU", header_format)
	totals.write(17, 0, "Other", header_format)

	#writing the totals
	HeaderCount = 1
	RowCount = 11
	Header_List = [{"header":'QA Type'}]
	for i in updated_Totals:
		for j in i:
			if year in j:
				totals.write(10, HeaderCount, j, header_format)
				HeaderCount += 1
				RowCount = 11
				Header_List.append({"header":j})
			if year not in j:
				totals.write(RowCount, HeaderCount-1, j)
				RowCount += 1
	totals.add_table(10,0, RowCount-1, HeaderCount-1, {'columns':Header_List})

	#I could have probably written a loop for this, but that's the next version - I wanted to really wrap my head around this first
	totals.write(19, 0, "Amount of QAs Assigned", header_format)
	totals.write(21, 0, "S", header_format)
	totals.write(22, 0, "P", header_format)
	totals.write(23, 0, "A", header_format)
	totals.write(24, 0, "Nolan Kingdon", header_format)
	totals.write(25, 0, "Ph", header_format)
	totals.write(26, 0, "D", header_format)
	totals.write(27, 0, "St", header_format)
	totals.write(28, 0, "R", header_format)
	totals.write(29, 0, "Z", header_format)
	totals.write(30, 0, "D", header_format)
	totals.write(31, 0, "L", header_format)
	totals.write(32, 0, "Di", header_format)

	HeaderCount = 1
	RowCount = 21
	Header_List = [{"header":'Specialist'}]
	for i in updated_Assigned:
		for j in i:
			if year in j:
				HeaderCount +=1
				RowCount = 21
				Header_List.append({"header":j})
			else:
				totals.write(RowCount, HeaderCount-1, j)
				RowCount +=1
	totals.add_table(20,0, RowCount-1, HeaderCount-1, {'columns':Header_List})

	totals.write(33, 0, "Amount of QAs Picked Up", header_format)
	totals.write(35, 0, "S", header_format)
	totals.write(36, 0, "P", header_format)
	totals.write(37, 0, "A", header_format)
	totals.write(38, 0, "Nolan Kingdon", header_format)
	totals.write(39, 0, "Ph", header_format)
	totals.write(40, 0, "D", header_format)
	totals.write(41, 0, "St", header_format)
	totals.write(42, 0, "R", header_format)
	totals.write(43, 0, "Z", header_format)
	totals.write(44, 0, "D", header_format)
	totals.write(45, 0, "L", header_format)
	totals.write(46, 0, "Di", header_format)

	HeaderCount = 1
	RowCount = 35
	Header_List = [{"header":'Specialist'}]
	for i in updated_PickedUp:
		for j in i:
			if year in j:
				HeaderCount +=1
				RowCount = 35
				Header_List.append({"header":j})
			else:
				totals.write(RowCount, HeaderCount-1, j)
				RowCount +=1
	totals.add_table(34,0, RowCount-1, HeaderCount-1, {'columns':Header_List})

	totals.write(47, 0, "Amount of QAs Put Up", header_format)
	totals.write(49, 0, "S", header_format)
	totals.write(50, 0, "P", header_format)
	totals.write(51, 0, "A", header_format)
	totals.write(52, 0, "Nolan Kingdon", header_format)
	totals.write(53, 0, "Ph", header_format)
	totals.write(54, 0, "D", header_format)
	totals.write(55, 0, "St", header_format)
	totals.write(56, 0, "R", header_format)
	totals.write(57, 0, "Z", header_format)
	totals.write(58, 0, "D", header_format)
	totals.write(59, 0, "L", header_format)
	totals.write(60, 0, "Di", header_format)

	HeaderCount = 1
	RowCount = 49
	Header_List = [{"header":'Specialist'}]
	for i in updated_PutUp:
		for j in i:
			if year in j:
				HeaderCount +=1
				RowCount = 49
				Header_List.append({"header":j})
			else:
				totals.write(RowCount, HeaderCount-1, j, header_format)
				RowCount +=1
	totals.add_table(48,0, RowCount-1, HeaderCount-1, {'columns':Header_List})
#MAIN
readCSV_Clone = []
#Reading the CSV
input_name = str(input("Which CSV file did you want to open?\nAlternatively, type 'skip' to just print the xlsx\n"))
if input_name != 'skip':
	input_name_full = input_name + ".csv"
	with open(input_name_full, 'r') as export:
		readCSV = list(csv.reader(export, delimiter = ','))
		#Need a clone - Is the most memory efficient - Alternative is keeping the CSV open longer
		readCSV_Clone = readCSV
		metrics(readCSV)
	export.close()

#Writing to the DB
Database_Name = str(input("DB You want to connect to/create?\n"))
Database_Name_Full = Database_Name + '.db'
conn = sqlite3.connect(Database_Name_Full)
c = conn.cursor()

#Creating/Writing Current Info
if input_name != 'skip':
	create_tables()
	data_entry_Metrics(date, totalQA, assignedQA, unAss, avgTUC, avgTUD)
	data_entry_Totals(date, totalInitial, totalDelivery, totalLaunch, totalSC, totalPayment, totalYRU, totalOther)
	data_entry_AssignedQA(date, ACount)
	data_entry_PutUpQA(date, PutCount)
	data_entry_PickedUpQA(date, PUCount)
	data_entry_Specialists(date, readCSV_Clone)

#Now we read from the DB and write the XLSX

updated_Assigned = read_from_db_AssPickPut("AssignedQA")
updated_PickedUp = read_from_db_AssPickPut("PickedUpQA")
updated_PutUp = read_from_db_AssPickPut("PutUpQA")
updated_Totals = read_from_db_Totals()
updated_Metrics = read_from_db_Metrics()

#creating the xlsx file for today's date
workbook = xlsxwriter.Workbook(date + '.xlsx')
#Creating the totals sheet
totals = workbook.add_worksheet("Totals")
#Setting totals sheet to a specific size
totals.set_column('A:Z', 18)
#Creating a second sheet for the weekly data
allData = workbook.add_worksheet("Weekly Raw Data")
#setting the format for the header row to add in to my loops
header_format = workbook.add_format({'bold': True})
#adding in the formatting for the title
title_format = workbook.add_format({'bold': True, 'font_size': 20, 'underline': True})
#writing the xlsx file
write_report(date, year, updated_Metrics, updated_Totals, updated_PutUp, updated_PickedUp, updated_Assigned, allData, totals, workbook, header_format, title_format)

#Closing xlsx
workbook.close()
#closing SQLite3
c.close
conn.close()


