# QA-Reporting-Tool
QA Reporting tool for weekly metrics

Currently includes the old, first draft (Reporter.py) and the rewrite that is more dynamic and better organized.

Names were truncated for the sake of keeping my coworkers anonymous

* Reads standardized CSV Export template containing Raw data based on QA responsibilities
* Organizes raw data into a readable format, stores in local memory
* Pushes the local information to SQLite3 Database
* Reads cumulative Data from SQLite3 Database
* Organizes and writes the information to a table in an XLSX file - Based on Week of report to allow for long term tracking
