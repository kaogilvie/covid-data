from datetime import date
from datetime import timedelta

today_date = date.today()-timedelta(2)

sql_dict = {
    "total_cases_by_state": f"""SELECT date, state, cases, ROW_NUMBER() OVER (PARTITION BY (date) ORDER BY cases desc) as rank
                        FROM nytimes.total_cases_by_state
                        WHERE date = '{today_date}'""",
    "total_deaths_by_state": f"""SELECT date, state, deaths, ROW_NUMBER() OVER (PARTITION BY (date) ORDER BY deaths desc) as rank
                        FROM nytimes.total_cases_by_state
                        WHERE date = '{today_date}'""",
    "totals_by_state": f"""SELECT date, state, deaths, cases, ROW_NUMBER() OVER (PARTITION BY (date) ORDER BY cases desc) as rank
                        FROM nytimes.total_cases_by_state
                        WHERE date = '{today_date}'""",
    "daily_by_state": f"""SELECT date, state, deaths, cases
                        FROM nytimes.daily_by_state
                       """,
    "daily_countrywide": f"""SELECT date, sum(deaths) as deaths, sum(cases) as cases
                            FROM nytimes.daily_by_state
                            GROUP BY date"""
}

### NEED TO ADD THESE TO THE FULL FLOW
