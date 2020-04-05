Docker repo to assist in rapid analysis and development on top of existing data for COVID-19.

Runs local Postgres instance and populates [NYTimes dataset](https://github.com/nytimes/covid-19-data) into tables that
can be easily used for analysis. There's DDL and everything.

Will be extending this to other datasets to provide a quickstart to anyone who
wants to play around with tons of COVID-19 datasets not in a CSV format.

### LOCAL SETUP
- Add COVID_DB_USER & COVID_DB_PASSWORD to your environment variables (highly suggest
  putting this into your .profile / .bashrc / .bash_profile files). If you want to
  change the postgres db name from 'covid' to something else, update the connect.py file
  in covid_utils. If you'd like to update the postgres port, you need to update the connect.py
  file in covid_utils in addition to the docker-compose file.
- If you'd like to load data, clone the necessary repos (see the "data" section),
then check out the local_config.py file. This file contains the place where you'd
hardcode paths for your local machine. You can be as abstract or as deliberate as you like.
- If you have a local Postgres install, make sure it isn't masking the ports that
the docker-compose file exposes. I ran into this problem on OSX and just spun down
the homebrew server that kept restarting -- `brew services postgres stop`
- For now, you'll need to run the DDL file corresponding to the datasource you want to look at after the initial
Postgres setup. You can find those files in the DDL folder.

### Data Sources
- [NYTimes dataset](https://github.com/nytimes/covid-19-data)
  - State and county level timeseries of cases and deaths since January 2020


### Work with the data already!
#### Start Databases
`docker-compose up`

#### Load data
`cd /path/to/covid/repo
python load_data/{source}_data.py`
