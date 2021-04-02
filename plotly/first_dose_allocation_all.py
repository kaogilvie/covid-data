import os
import logging

from chart_studio import plotly as py
import plotly.graph_objects as go
import requests
import pandas as pd

from covid_utils import logs

logs.configure_logging('VaccinationTrackerLogger')
logger = logging.getLogger()

logger.info("Signing into plotly.")
py.sign_in('tkogilvie', os.environ['PLOTLY_TOKEN'])
logger.info("Signed in.")

logger.info(f"Gathering vaccine data from CDC API.")

cdc_headers = {'X-App-Token': os.environ['CDC_VAX_APP_TOKEN']}

logger.info("Getting Moderna data...")
moderna_url = 'https://data.cdc.gov/resource/b7pe-5nws.json'
moderna_response = requests.get(moderna_url, headers=cdc_headers)
moderna = pd.read_json(moderna_response.text, orient='records')

logger.info("Getting Pfizer data...")
pfizer_url = 'https://data.cdc.gov/resource/saz5-9hgg.json'
pfizer_response = requests.get(pfizer_url, headers=cdc_headers)
pfizer = pd.read_json(pfizer_response.text, orient='records')
pfizer_usa = pfizer.groupby('week_of_allocations').sum().reset_index()

logger.info("Getting J&J data...")
jandj_url = 'https://data.cdc.gov/resource/w9zu-fywh.json'
jandj_response = requests.get(jandj_url, headers=cdc_headers)
jandj = pd.read_json(jandj_response.text, orient='records')
jandj_usa = jandj.groupby('week_of_allocations').sum().reset_index()

logger.info("Getting national totals...")
moderna_usa = moderna.groupby('week_of_allocations').sum().reset_index()
pfizer_usa = pfizer.groupby('week_of_allocations').sum().reset_index()
jandj_usa = jandj.groupby('week_of_allocations').sum().reset_index()

logger.info("Obtaining jurisdiction list...")
jurisdiction_list = list(moderna.jurisdiction.unique())
jurisdiction_list.sort()
dropdown_list = ['USA']
dropdown_list.extend(jurisdiction_list)
logger.info("Creating filter...")
button_actions = []
for idx, jurisdiction in enumerate(dropdown_list):
    visibility_list = [False]*(len(dropdown_list)*3)
    visibility_list[idx*3] = True
    visibility_list[idx*3+1] = True
    visibility_list[idx*3+2] = True
    button_actions.append({
        'label': jurisdiction,
        'method': 'update',
        'args': [{'visible': visibility_list}]
    })

button_actions[0]

logger.info("Adding a trace for each geo.")
fig = go.Figure()
for jurisdiction in dropdown_list:
    if jurisdiction == 'USA':
        moderna_filtered = moderna_usa
        pfizer_filtered = pfizer_usa
        jandj_filtered = jandj_usa
        visible = True
    else:
        moderna_filtered = moderna[moderna['jurisdiction']==jurisdiction]
        pfizer_filtered = pfizer[pfizer['jurisdiction']==jurisdiction]
        jandj_filtered = jandj[jandj['jurisdiction']==jurisdiction]
        visible = False

    fig.add_trace(go.Bar({
                "name": "Moderna",
                'type': 'bar',
                'x': moderna_filtered['week_of_allocations'],
                'y': moderna_filtered['_1st_dose_allocations'],
                "marker": {
                    'color': '#636EFA'
                },
                "hovertemplate": "Week of %{x} <br>Moderna Doses: %{y}",
                "visible": visible
            })
        )

    fig.add_trace(go.Bar({
                "name": "Pfizer",
                'type': 'bar',
                'x': pfizer_filtered['week_of_allocations'],
                'y': pfizer_filtered['_1st_dose_allocations'],
                "marker": {
                    'color': '#EF553B'
                },
                "hovertemplate": "Week of %{x} <br>Pfizer Doses: %{y}",
                "visible": visible
            })
        )

    fig.add_trace(go.Bar({
                "name": "J&J",
                'type': 'bar',
                'x': jandj_filtered['week_of_allocations'],
                'y': jandj_filtered['_1st_dose_allocations'],
                "marker": {
                    'color': '#00CC96'
                },
                "hovertemplate": "Week of %{x} <br>J&J Doses: %{y}",
                "visible": visible
            })
        )



logger.info("--Most recent dates of data--")
logger.info(f"Moderna: {moderna['week_of_allocations'].max()}")
logger.info(f"Pfizer: {pfizer['week_of_allocations'].max()}")
logger.info(f"J&J: {jandj['week_of_allocations'].max()}")

logger.info("Creating the chart...")
vaccine_layout = {
  "title": {"text": "First Dose Vaccination Allocation"},
  "xaxis": {
    "type": "date",
    "title": {"text": "Date When Allocation Becomes Available"},
    "autorange": True
  },
  "yaxis": {
    "type": "linear",
    "title": {"text": "Total Vaccine Allocation"},
    "autorange": True
  },
  "barmode": "stack",
  "autosize": True,
  "updatemenus": [{"buttons" : button_actions, "active": 0, "visible": True}]
}


fig.update_layout(vaccine_layout)

logger.info("Publishing chart.")
plot_url = py.plot(fig, filename='First-Dose_Vaccine_Allocation', auto_open=False)
logger.info(f"Chart published at {plot_url}.")
