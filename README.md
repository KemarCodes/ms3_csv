This standalone JAVA program parse the data from a CSV file and inserts it into an embedded sql database.

This was built using gradle and JAVA 1.8

Command to run: java -jar CSVProcess.jar '<pathToFile>'

The output files will be created at the location of the input CSV.

Used only one JAVA class to process the CSV, manage the database connection, and create output files. Since there wasn't
much code needed for this I took more a procedural approach than an OOP one. If this task was larger I would take a
more abstract approach for maintainability. This is first time using SQLite and its also been a while since I worked with
any JAVA database connection that wasn't Mybatis. 

The assumptions I made based on the requirements were:
 1. Empty cells in a row doens't make it invalid as long as it produces the right number of columns
 2. Deleting the data in the database, ...-bad.csv, and log file is ok if we are rerunning with the same input file
 3. Using the text datatype for all the columns is alright since no data types for the columns are specified and no
    assurances are given about the data.
 4. Noticed there was a row other than the first row that contained the header names. Assumed it was allowed and no extra 
	validation was needed for it. 
 
The successful statistic is based on how many rows from the CSV were read into a list of validated rows. I check to see
how many rows get inserted in the database, but only deliver a warning in the log file if the number of rows in the database
and number of rows successfully parsed from the CSV don't match. Should really be updating the ...-bad.csv, successful
statistic, and failed statistic if a row doesn't insert, but didn't see a simple way to do that with batch command being
used and I couldn't think of any standout reason why these inserts would fail.






