# QA-Reporting-Tool
QA Reporting tool for weekly metrics

* Reads standardized CSV Export template containing Raw data based on QA responsibilities
* Organizes raw data into a readable format, stores in local memory
* Pushes the local information to SQLite3 Database
* Reads cumulative Data from SQLite3 Database
* Organizes and writes the information to a table in an XLSX file - Based on Week of report to allow for long term tracking

## Testing
To test, download the test data in the testdata folder. Information has been altered to be more confidential.

The script itself is designed to be run **weekly** - Running twice in one day will cause issues in the xlsx and duplicate headers in the pivot table. Change the 'date' argument to a string of a date formatted "dd-mm-yyyy'each run to avoid this.
