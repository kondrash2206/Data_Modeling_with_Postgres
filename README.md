# Data_Modeling_with_Postgres
Data Modeling using Postgress and Python

# Description
In this project I have transferred data from .json files into Postgresql Database using Python library psycopg2. Json files were obtained from ["Million Song Dataset"](http://millionsongdataset.com/) and ["Music streaming app event simulator"](https://github.com/Interana/eventsim). As a result I have created a database and ETL Pipeline of an imaginary music streaming app. 
This database and ETL Pipeline are of huge benefit for this sort of app due to following reasons:
* Provides information about user activities over times of day and user location which makes possible to correctly arrange resources
* Provides information about user song and band preferences which allow to develop a recommendation system (rank based recomendations, user-user collaborative filtering e.t.c)
* Provides information about user subscriptions. The behaviour of users that cancelled the subscription can be analysed and this knowldege can be used to identify users that might canceln their membership in the future. Such users could recieve some benefits (discounts, gifts etc) which prevents the cancelation of their membership. 

# Database Schema 
The star schema below allows to answer the above stated questions:
![](https://github.com/kondrash2206/Data_Modeling_with_Postgres/blob/master/schema.png)

# ETL Pipeline
ETL Pipeline gets the data from 2 datasources and fills 4 Tables (Songs, Artists, Time, User). The Songplays Table is filled also from Songsand Artists Tables.
![](https://github.com/kondrash2206/Data_Modeling_with_Postgres/blob/master/ETL.png)


# Files
* **sql_queries.py** -collection of all SQL Querries used in table manipulation and etl
* **create_tables.py** - file that creates a database, drops all existing tables and creates new tables
* **etl.py** - ETL (Extract Transfer Load) Pipeline that fills the previously created tables with data from json files

# Installations
In order to run this project following python libraries are needed: psycopg2, pandas

### Acknowledgements
This project is a part of Udacity "Data Engineering" Nanodegree
