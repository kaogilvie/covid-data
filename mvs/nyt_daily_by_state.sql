drop table if exists nytimes.daily_by_state;
  create table nytimes.daily_by_state AS (
    select date, state, (sub.cases - COALESCE(sub.prev_cases, 0)) as new_cases, (sub.deaths - COALESCE(sub.prev_deaths,0)) as new_deaths
              from (
                select date,
                state,
                cases,
                LAG(cases, 1) OVER(PARTITION BY (state) ORDER BY (date)) as prev_cases,
                deaths,
                LAG(deaths, 1) OVER(PARTITION BY (state) ORDER BY (date)) as prev_deaths
                from nytimes.total_cases_by_state
              ) as sub
  );
