config = {
    'nyt_daily_by_state':
        {
            'output_file': 'daily_by_state.csv',
            'sql_file': 'nyt_daily_by_state.sql',
            'aux_alterations': True,
            'output_schema': 'nytimes',
            'output_table': 'daily_by_state'
        },
    'nyt_totals_by_state':
        {
            'output_file': 'totals_by_state.csv',
            'sql_file': 'nyt_county_geo.sql',
            'aux_alterations': False,
            'output_schema': 'nytimes',
            'output_table': 'total_cases_county_geo'
        }
}
