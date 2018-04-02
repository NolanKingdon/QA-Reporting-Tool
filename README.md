# QA-Reporting-Tool
QA Reporting tool for weekly metrics

* Reads standardized CSV Export template containing Raw data based on QA responsibilities
* Organizes raw data into a readable format, stores in local memory
* Pushes the local information to SQLite3 Database
* Reads cumulative Data from SQLite3 Database
* Organizes and writes the information to a table in an XLSX file - Based on Week of report to allow for long term tracking

## Testing
To test, download the test data in the testdata folder. Information has been altered to be more confidential.

The script itself is designed to be run **weekly** - Running twice in one day will cause issues in the xlsx and duplicate headers in the pivot table. Change the 'date' argument (in: SQLite Read/Write Functions - Line 528)to a string of a date formatted "dd-mm-yyyy'each run to avoid this.

#### Important Notes about testing

* When asked for inputs, file names are __without__ the file extension
 * TestData1.csv inputs as TestData1.
 * Your database is entered in the same way - __NewDatabase__ as an input creates __NewDatabase.db__ in the directory
* File name is pulled from today's date
* Column header is pulled from todays date in the XLSX output - see above for how to avoid errors related to this when testing
