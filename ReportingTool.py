import sqlite3
import time
import datetime
import csv
from functools import reduce
import xlsxwriter

''' VARIABLES '''
#DataBase connections created
#For some reason c isn't recognized in other functions when put in main even when it's passed as an argument. Trying it here
Database_Name = str(input("DB You want to connect to/create?\n\n"))
Database_Name_Full = Database_Name + '.db'
conn = sqlite3.connect(Database_Name_Full)
c = conn.cursor()

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
	#Args will be Name_Bank
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

def all_names(csvNames, dbNames):
	#Decided not to write straight to dbNames for clarity's sake
	Name_Bank = dbNames

	for name in csvNames:
		if name not in Name_Bank:
			Name_Bank.append(name)
	Name_Bank.sort()
	return Name_Bank

def create_tables_statics():
	#Creating the table for the less dynamic aspects (Tables that I know I Want to collect to)
	Table_Bank = ["AssignedQA","PickedUpQA","PutUpQA"]
	c.execute("CREATE TABLE IF NOT EXISTS 'Metrics'('Date' TEXT, TotalQA TEXT, AssignedQA TEXT, UnAssigned TEXT, AverageTimeUntilCompleted TEXT, AverageTimeUntilDueDate TEXT)")
	c.execute("CREATE TABLE IF NOT EXISTS 'Totals'('Date' TEXT, Initial TEXT, Delivery TEXT, Launch TEXT, SchoolChoice TEXT, Payment TEXT, YRU TEXT, Other TEXT)")
	c.execute("CREATE TABLE IF NOT EXISTS 'PutUpQA'('Date' TEXT)")
	c.execute("CREATE TABLE IF NOT EXISTS 'PickedUpQA'('Date' TEXT)")
	c.execute("CREATE TABLE IF NOT EXISTS 'AssignedQA'('Date' TEXT)")

def create_tables_dynamics(nameBank):
	#Creating the table for all the dynamic names (Names that may not be already in the DB)
	for i in nameBank:
		c.execute("CREATE TABLE IF NOT EXISTS '" + i + "'('Date' TEXT, 'Taken By' TEXT, DueDate TEXT, Netsuite TEXT, LeadSpecialist TEXT, District TEXT, Year TEXT, Solution TEXT, QAType TEXT, SIS TEXT, ENT TEXT, Payment TEXT, Localization TEXT, SC TEXT, SSO TEXT, SchoolLocator TEXT, NorthCarolina TEXT, EmailHistory TEXT, CompletedDate TEXT, Submitted TEXT, qa_Assigned TEXT)")

def append_table_cols(specialist):
	tables = ['AssignedQA','PickedUpQA','PutUpQA']
	for table in tables:
		for name in specialist:
			try:
				#Fix this in the future with better SQL to avoid the try/except maybe?
				c.execute("ALTER TABLE " + table + " ADD COLUMN'" + name + "' TEXT")
			except:
				#If they're already in the DB, I really don't want anything to happen
				pass

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
	Dynamic_Names = ""
	Dynamic_Values = "?, "
	List_Data = [today]

	for i in Assign_Count:
		if type(i) == int:
			List_Data.append(i)
			Dynamic_Values +="?, "
		if type(i) == str:
			Dynamic_Names += ("'" + str(i) + "', ")

	#SQLite3 takes a tuple argument for the inputs to the DB
	Tuple_Data = tuple(List_Data)

	c.execute("INSERT INTO AssignedQA('Date', " + Dynamic_Names[:-2] + ") VALUES (" + Dynamic_Values[:-2] + ")", Tuple_Data)
	conn.commit()

def data_entry_PickedUpQA(today, Pickup_Count):
	#This needs to happen AFTER the name cols in the DB have been updated	
	Dynamic_Names = ""
	Dynamic_Values = "?, "
	List_Data = [today]

	for i in Pickup_Count:
		if type(i) == int:
			List_Data.append(i)
			Dynamic_Values +="?, "
		if type(i) == str:
			Dynamic_Names += ("'" + str(i) + "', ")

	#SQLite3 takes a tuple argument for the inputs to the DB
	Tuple_Data = tuple(List_Data)

	c.execute("INSERT INTO PickedUpQA('Date', " + Dynamic_Names[:-2] + ") VALUES (" + Dynamic_Values[:-2] + ")", Tuple_Data)
	conn.commit()
    
def data_entry_PutUpQA(today, Putup_Count):
	Dynamic_Names = ""
	Dynamic_Values = "?, "
	List_Data = [today]

	for i in Putup_Count:
		if type(i) == int:
			List_Data.append(i)
			Dynamic_Values +="?, "
		if type(i) == str:
			Dynamic_Names += ("'" + str(i) + "', ")
	#SQLite3 takes a tuple argument for the inputs to the DB
	Tuple_Data = tuple(List_Data)
	c.execute("INSERT INTO PutUpQA('Date', " + Dynamic_Names[:-2] + ") VALUES (" + Dynamic_Values[:-2] + ")", Tuple_Data)
	conn.commit()

def data_entry_Specialists(today, csv_File):
	#This is probably fine TBH
	for row in csv_File[1:]:
		c.execute('INSERT INTO "' + row[3] + '"("Date", "Taken By", "DueDate", Netsuite, LeadSpecialist, District, Year, Solution, QAType, SIS, ENT, Payment, Localization, SC, SSO, SchoolLocator, NorthCarolina, EmailHistory, CompletedDate, Submitted, qa_Assigned) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
		(today, row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13], row[14], row[15], row[16], row[17], row[18], row[19]))
	conn.commit()

''' CSV READ '''
def name_read(csvFile):
	unique_names = []
	csv_no_header = csvFile[1::]
	for row in csv_no_header:
		if row[0] not in unique_names and row[0] != "":
			unique_names.append(row[0])
		if row[3] not in unique_names:
			unique_names.append(row[3])
	return unique_names

def metrics(readCSV, nameBank):
	'''
	Consider splitting this into two or more functions? Would make it tons more manageable.
	'''
	#Writing everything once as a global seemed the most convenient in this case.
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
	
	#Tracking who picked up QAs
	for ass in readCSV:
		if ass[0] in nameBank and ass[-1] =="1":
			Name_Place = Assign_Count.index(ass[0])
			Assign_Count[Name_Place+1] +=1
	#Tracking who was assigned QAs
	for row in readCSV:
		if row[0] in nameBank and row[-1] == "0":
			Name_Place = Pickup_Count.index(row[0])
			Pickup_Count[(Name_Place+1)] += 1
	#Tracking who put in QAs
	for row in readCSV:
		if row[3] in nameBank:
			Name_Place = Putup_Count.index(row[3])
			Putup_Count[(Name_Place+1)] += 1

	iter(readCSV)
	#Dates
	for i in readCSV[1:]:
		time_Until_Due(i[-2],i[1])
	amtOfDueDates = len(Time_Due)
	Average_Due_Time = round((reduce((lambda x,y: x+y), Time_Due))/amtOfDueDates, 2)
	iter(readCSV)

	for i in readCSV[1:]:
		time_Until_Complete(i[-2],i[-3])
	amtOfCompletes = len(Time_Complete)
	Average_Complete_Time = round((reduce((lambda x,y: x+y), Time_Complete))/amtOfCompletes, 2)
	iter(readCSV)

def time_Until_Complete(inTime, completeTime):
	global Time_Complete

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
			Time_Complete.append(correctDate)
		elif difDate < 0 and (iM not in thirtyMonths and iM != 2):
			correctDate = 30 + difDate
			Time_Complete.append(correctDate)
		elif difDate < 0 and iM == 2:
			correctDate = 28 + difDate
			Time_Complete.append(correctDate)
		else:
			Time_Complete.append(difDate)
#Function for Closeness to Deadline on submission
def time_Until_Due(inTime, outTime):
	global Time_Due
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
		Time_Due.append(correctDate)
	elif difDate < 0 and (iM not in thirtyMonths and iM != 2):
		correctDate = 30 + difDate
		Time_Due.append(correctDate)
	elif difDate < 0 and iM == 2:
		correctDate = 28 + difDate
		Time_Due.append(correctDate)
	else:
		Time_Due.append(difDate)

''' SQL READ '''

def db_name_read(c):
	c.execute('SELECT * FROM PutUpQA')
	names = list(map(lambda x: x[0], c.description))
	names.remove('Date')
	return names

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
def write_report(date, year, nameBank, updated_Metrics, updated_Totals, updated_PutUp, updated_PickedUp, updated_Assigned, allData, totals, workbook,header_format, title_format):
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

	#AMOUNT OF QAS ASSIGNED

	totals.write(19, 0, "Amount of QAs Assigned", header_format)
	#This is the starting point for the line var.
	#It is currently 21 (Because of the metrics above taking up x amount of space)	
	lineStatic = 21
	lineDynamic = 21
	
	for i in nameBank:
		totals.write(lineDynamic, 0, i, header_format)	
		lineDynamic +=1
		#We end off here with the line value being around 32 (Where it was before)
	HeaderCount = 1
	#Wanna make sure our rows are starting at 21 too
	RowCount = lineStatic
	Header_List = [{"header":'Specialist'}]
	
	for i in updated_Assigned:
		for j in i:
			if j == None:
				j = "0"
			if year in j:
				HeaderCount +=1
				#Wanna make sure the rows go back to 21
				RowCount = lineStatic
				Header_List.append({"header":j})
			else:
				totals.write(RowCount, HeaderCount-1, j)
				RowCount +=1
				#We are ending this with the rowcount at roughtly 32
	
	totals.add_table(lineStatic-1,0, RowCount-1, HeaderCount-1, {'columns':Header_List})
	#Lets also add 1 to our dynamic count so we can write another line down
	lineDynamic += 1
	totals.write(lineDynamic, 0, "Amount of QAs Picked Up", header_format)
	#It was at 33 for the title write, now we are 35 for the specialist writes
	lineDynamic +=2
	#We need a reference at 35
	lineStatic = lineDynamic
	for i in nameBank:
		totals.write(lineDynamic, 0, i, header_format)	
		lineDynamic +=1
	
	#lineDynamic is now 46
	HeaderCount = 1
	RowCount = lineStatic
	Header_List = [{"header":'Specialist'}]
	
	for i in updated_PickedUp:
		for j in i:
			if j == None:
				j = "0"
			if year in j:
				HeaderCount +=1
				RowCount = lineStatic
				Header_List.append({"header":j})
			else:
				totals.write(RowCount, HeaderCount-1, j)
				RowCount +=1
	#Referencing one BEFORE lineStatic to make sure we get everything
	totals.add_table(lineStatic-1 ,0, RowCount-1, HeaderCount-1, {'columns':Header_List})

	lineDynamic += 1
	totals.write(lineDynamic, 0, "Amount of QAs Put Up", header_format)
	lineDynamic += 2
	lineStatic = lineDynamic

	for i in nameBank:
		totals.write(lineDynamic, 0, i, header_format)	
		lineDynamic +=1
	HeaderCount = 1
	#We want rowCount to be 35. which is what lineDynamic WAS.
	RowCount = lineStatic
	Header_List = [{"header":'Specialist'}]
	for i in updated_PutUp:
		for j in i:
			if j == None:
				j = "0"
			if year in j:
				HeaderCount +=1
				RowCount = lineStatic
				Header_List.append({"header":j})
			else:
				totals.write(RowCount, HeaderCount-1, j, header_format)
				RowCount +=1
	totals.add_table(lineStatic-1,0, RowCount-1, HeaderCount-1, {'columns':Header_List})

''' MAIN '''
def main():
	#CSV File read
	while True:
		try:
			input_name = str(input("Which CSV file did you want to open?\n\n"))
			if input_name != 'skip':
				input_name_full = input_name + ".csv"
				with open(input_name_full, 'r') as export:
					readCSV = list(csv.reader(export, delimiter = ','))
					readCSV_Clone = readCSV
					csv_names = name_read(readCSV)
				export.close()
			break
		except:
			print("\nInvalid file name, please try again (Just use the file name, not the .csv extension)\n")
			continue
	'''Initialization Functions'''
	create_tables_statics()
	#Getting DB names in PutUpQA (All names are stored here)
	db_names = db_name_read(c)
	#Getting ALL names new or old
	Name_Bank = all_names(csv_names, db_names)
	append_table_cols(Name_Bank)
	create_list_data(Name_Bank)
	create_tables_dynamics(Name_Bank)
	'''Data Creation Function'''
	metrics(readCSV, Name_Bank)
	'''SQLite Read/Write Functions'''
	data_entry_Metrics(date, Total_QA, Total_Assigned, Total_Assigned, Average_Complete_Time, Average_Due_Time)
	data_entry_Totals(date, Total_Initial, Total_Delivery, Total_Launch, Total_SC, Total_Payment, Total_YRU, Total_Other)
	data_entry_AssignedQA(date, Assign_Count)
	data_entry_PickedUpQA(date, Pickup_Count)
	data_entry_PutUpQA(date, Putup_Count)
	data_entry_Specialists(date, readCSV)
	#Bring in Cumulative SQL info with reads
	Updated_AssignedQA = read_from_db_AssPickPut("AssignedQA")
	Updated_PutUpQA = read_from_db_AssPickPut("PutUpQA")
	Updated_PickedUpQA = read_from_db_AssPickPut("PickedUpQA")
	Updated_Metrics = read_from_db_Metrics()
	Updated_Totals = read_from_db_Totals()

	'''XLSX Writer Functions'''
	workbook = xlsxwriter.Workbook(date + '.xlsx')
	totals = workbook.add_worksheet("Totals")
	totals.set_column('A:Z', 18)
	allData = workbook.add_worksheet("Weekly Raw Data")
	header_format = workbook.add_format({'bold': True})
	title_format = workbook.add_format({'bold': True, 'font_size': 20, 'underline': True})
	write_report(date, year, Name_Bank, Updated_Metrics, Updated_Totals, Updated_PutUpQA, Updated_PickedUpQA, Updated_AssignedQA, allData, totals, workbook, header_format, title_format)

	print("XLSX form is ready.\nPlease check the same location of the exe file.\nFile will be named today's date")
	time.sleep(6)
	'''End of Script'''

if __name__ == "__main__":
	main()
