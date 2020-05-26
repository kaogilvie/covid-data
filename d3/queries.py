from datetime import date
from datetime import timedelta

today_date = date.today()-timedelta(1)

sql_dict = {
    "total_cases_by_state": f"""SELECT date, state, cases, ROW_NUMBER() OVER (PARTITION BY (date) ORDER BY cases desc) as rank
                        FROM nytimes.total_cases_by_state
                        WHERE date = '{today_date}'""",
    "total_deaths_by_state": f"""SELECT date, state, deaths, ROW_NUMBER() OVER (PARTITION BY (date) ORDER BY deaths desc) as rank
                        FROM nytimes.total_cases_by_state
                        WHERE date = '{today_date}'""",
    "totals_by_state": f"""SELECT date, state, deaths, cases, ROW_NUMBER() OVER (PARTITION BY (date) ORDER BY cases desc) as rank
                        FROM nytimes.total_cases_by_state
                        WHERE date = '{today_date}'"""
}
