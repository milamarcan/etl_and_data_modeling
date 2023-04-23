# etl_and_data_modeling

## Project overview
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They would like to have a Postgres database with tables designed to optimize queries on song play analysis. The goal is to create a database schema and ETL pipeline for this analysis. The database and ETL pipeline also should be tested by running queries given by the analytics team from Sparkify.

ETL pipeline is done using Python.

## Logical Data Model for the project
![Data Model for ETL Sparkify project](/img/DataModelSparkify.png)

## Overview of the files in the repository
- 'create_tables.py': drops and creates tables.
- 'etl.ipynb' Jupyter notebook file: reads and processes a single file from both songs and logs files and loads that data into the tables. Was used as a pre step before running 'etl.py' file. 
- 'etl.py' file: reads and processes all files from songs and logs files and loads the data into the tables.
- 'sql_queries.py' file: contains all SQL queries, and is imported into the 'create_tables.py', 'etl.ipynb' and 'etl.py' files.
- 'test.ipynb' Jupyter file: displays the first few rows of each table to check the database.
- 'data' folder: contains logs and songs files (data sources for the current project).
- 'img' folder: contains images for the current file.
- 'requirements.txt' file: contains a list of dependencies for the project.
- 'database.cfg.template' file: contains default configuration of the databases that are used. 

## Running the project 

### Pre-requisites:
- Create a new virtual environment for the project (optional but highly recommended) 
- Run 'requirements.txt' file to install dependencies for the project (pip install -r requirements.txt)

### How to run the project
1. Run 'create_tables.py' file. It will create output tables.
2. Run 'etl.py' to process all files from 'data' folder. 
3. If you want to see the tests, run 'test.ipynb' file.

## Output of the project
Current input songs data is a subset of the data from [Million Song Dataset](http://millionsongdataset.com/). 
Data contains only 1 row that matches requirements. 
![Sparkify Data Modeling project output](/img/Sparkify-output.jpg)
