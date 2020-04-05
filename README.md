Docker repo to assist in rapid analysis of existing data for COVID-19
and Kaggle NLP efforts.

Runs local Postgres instance and populates [NYTimes dataset](https://github.com/nytimes/covid-19-data) into tables that
can be easily used for analysis. There's DDL and everything.

Will be extending this to other datasets to provide a quickstart to anyone who
wants to play around with tons of COVID-19 datasets not in a CSV format.

### LOCAL SETUP
- If you'd like to load data, check out the local_config.py file. This file contains
the place where you'd hardcode paths for your local machine. You can be as abstract
or as deliberate as you like.
