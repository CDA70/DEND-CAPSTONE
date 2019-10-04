# Data Engineering Capstone Project
This is the final project in the Udacity data engineering course. I ran into some time constraints but at the end I decided to finish the project by collecting and generating my own dataset using the yahoo financials API. 

The dataset is a collection of stock ticker information of all S&P 500 companies stored in dimentional tables where ETL is used to create fact tables with several trading indicators. 

The project turned out as a great learning experience where I gained some basic knowledge in the world of using algorithms in financial mathematical trading.  

**disclaimer**: the data sets and algorithms are not tested and the results cannot be used in any way possible. The project is a pure learning experience which provided lots of fun. 

## environment
The project is developed using jupyter notebooks and python files. As the backend, redshift on AWS is used. The initial dataset is installed locally and additionally on a S3 bucket. Initially I used a virtual machine with Annaconda installed, but during the development I started exploring and using AWS SageMaker. SageMaker is an excellent environment and it doesn't require any configuration. SageMaker can simply be started from within your AWS account. In case you prefer to use jupyter notebook locally just download the repository and run `jupyter notebook` from a terminal.

  * The ticker data used in this project are provided by yahoo, using pandas-datareader
  * The company information is scrapped from wikipedia
  * All this data is converted in CSV and JSON files and further used to load the database on redshift 

## data model (see sql_queries.py for more details)
The datamodel is rather simple, but can be extended easily. The datamodel isn't very conventional with lots of PK, FK and relationships to query. The entire result can be restrieved from querying one of the fact tables
For this project I have two dimensional tables and three fact tables. 


![DM](/assets/dm.PNG)

### Dimentional tables: 
sp5oo_companies, and sp500_tickers. Both tables are loaded by using CSV files. The same can be done from JSON files. 
The datasets in CSV and JSON are generated using pandas datareader where ticker data is retrieved from yahoo. Each CSV is holding ticker data for one company since 2000. That means that we have 505 CSV and also JSON files stored in a S3 bucket that generates over 2 million records in the tickers table.

### Fact tables:
The project holds 3 fact tables, each fact table contains the of one trading indicator. The 3 fact tables are:
- sp500_SMA_30
- sp500_bollinger
- sp500_macd

Initially I used one fact table but I quickly decided to use a seperate fact tables for each trading indicator. The seperation ensures a very flexible way by only extending the datamodel without changing existing fact tables. It means, including a new trading indicator is as simple as creating a new table, and adding some additonal python functions that calculates the trading indicator. Also each fact tables hold more than 2 million records


# 1. Get started (dwh-redshift-cluster.ipynb)
First we create a redshift cluster. This is done by executing the steps in the `dwh-redshift-cluster.ipynb` notebook. It can easily be turned into a separate python class, but for this project I decided to use a notebook where I can run the cells individually.

The notebook actually creates a redshift cluster on AWS. 

# 2. create the tables (create_tables.py)
run: `python create_tables.py`

The procedures will create the tables in the running redshift cluster
  * first the tables are dropped
 
  * second the tables are created

# 3. Gather the data, prepate the dataset, and load the data into the different dimensional and fact tables
> as an example, I kept a few CSV in each folder to demonstrate the CSV and JSON structure

This process goes in two parts:

## 3.1. dimensional tables
The data of the of the dimensional tables are collected and stored in CSV files. 
A COPY command loads the data into the dimensional tables in the database. 

## 3.2. fact tables
The fact tables can be loaded as soon as the dimensional tables are loaded. But we first must create the CSV files before we load the tables.

> detailed information can be found in the notebook `prepare-finstart-data-ipynb`
The process to create the fact data in CSV and eventually to load the data in the database is a combined process by running some steps in the notebook `prepare-finstart-data.ipynb` and running the etl.py file in a terminal.

The etl.py loads either the dimensional tables or fact tables. It depends on the argument provided to the the etl.py
  * to copy the data from the csv files in the dimensional tables, run from a terminal: `python etl.py dim`
  * to copy the data from the csv files in the fact tables, run from a terminal: `python etl.py fact`

at the end the database tables are loaded and ready to be plotted for further analysis
![DATA](/assets/data.PNG)


# FINALLY PLOT THE DATA
open the notebook `test-and-plot-datasets.ipynb`

This part is extremely easy. the analyst only needs to understand the data columns provided in the fact tables. the analyst doesn't need to know joins or complex sql constructs. He or she can simply run a simple query and plot it with a favorite tool. I used matplotlib to plot some graphics.

A query can be as simple as ` select date, adj_close, sma_30 from public.sp500_sma30 where company = 'ABT' order by date asc `

showing a plot can become complex, but it can be as simple as:

``` python
%matplotlib inline

# sma_30 over the last 720 entries
df.tail(360).plot(figsize=(20, 5))

#sma_30 between a time period
startdate = pd.to_datetime("2019-04-01").date()
enddate = pd.to_datetime("2019-10-01").date()
df[startdate:enddate].plot(figsize=(20, 5))
```
![PLOTS](/assets/plots.PNG)

# Addressing Other Scenarios

## The data was increased by 100x.
As a start I ran into some time constraint due to several reasons, but initially I was also planning to output the result into PARQUET files where rows are presented in columns. A parquet is more efficient in terms of storage and performance.
Also the s3 bucket where the CSV and JSON files are stored can be structurly optimized and allows easier and faster uploads. At the moment the creation of the CSV and JSON files are not incremental. It's something that needs further improvement. As a next step I planned to restructure the S3 directory by splitting the date into a year. month and day. In that way a directory can be something as `/csv/2019/05/21/ABT.csv`... 

## The pipelines would be run on a daily basis by 7 am every day.
This project can be a good showcase to run on AIR FLOW. The airflow scheduler executes the different tasks on an array of workers while following the specified dependencies. It some that will continue to do in version 0.0.2

## The database needed to be accessed by 100+ people.
The database is already running on redshift where we could enable concurrency scaling. In that case redshift will add clusters if needed. Of course the data model is not very mature. It really can be improved by using proper sort key types etc. AWS has lots of recommendations to increase performance. 
