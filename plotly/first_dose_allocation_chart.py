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

target_jurisdiction = 'District of Columbia'
logger.info(f"Gathering vaccine data for {target_jurisdiction}.")

cdc_headers = {'X-App-Token': os.environ['CDC_VAX_APP_TOKEN']}

logger.info("Getting Moderna data...")
moderna_url = 'https://data.cdc.gov/resource/b7pe-5nws.json'
moderna_response = requests.get(moderna_url, headers=cdc_headers)
moderna = pd.read_json(moderna_response.text, orient='records')
moderna = moderna[moderna['jurisdiction']==target_jurisdiction]

logger.info("Getting Pfizer data...")
pfizer_url = 'https://data.cdc.gov/resource/saz5-9hgg.json'
pfizer_response = requests.get(pfizer_url, headers=cdc_headers)
pfizer = pd.read_json(pfizer_response.text, orient='records')
pfizer = pfizer[pfizer['jurisdiction']==target_jurisdiction]

logger.info("Getting J&J data...")
jandj_url = 'https://data.cdc.gov/resource/w9zu-fywh.json'
jandj_response = requests.get(jandj_url, headers=cdc_headers)
jandj = pd.read_json(jandj_response.text, orient='records')
jandj = jandj[jandj['jurisdiction']==target_jurisdiction]

jandj['week_of_allocations'].max()

logger.info("--Most recent dates of data--")
logger.info(f"Moderna: {moderna['week_of_allocations'].max()}")
logger.info(f"Pfizer: {pfizer['week_of_allocations'].max()}")
logger.info(f"J&J: {jandj['week_of_allocations'].max()}")

logger.info("Creating the chart...")
vaccine_layout = {
  "title": {"text": "First Dose Vaccination Allocation in DC"},
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
  "autosize": True
}

fig = go.Figure()
fig.add_trace(go.Bar({
            "name": "Moderna",
            'type': 'bar',
            'x': moderna['week_of_allocations'],
            'y': moderna['_1st_dose_allocations'],
            "marker": {
                'color': '#636EFA'
            },
            "hovertemplate": "Week of %{x} <br>Moderna Doses: %{y}"
        })
    )

fig.add_trace(go.Bar({
            "name": "Pfizer",
            'type': 'bar',
            'x': pfizer['week_of_allocations'],
            'y': pfizer['_1st_dose_allocations'],
            "marker": {
                'color': '#EF553B'
            },
            "hovertemplate": "Week of %{x} <br>Pfizer Doses: %{y}"
        })
    )

fig.add_trace(go.Bar({
            "name": "J&J",
            'type': 'bar',
            'x': jandj['week_of_allocations'],
            'y': jandj['_1st_dose_allocations'],
            "marker": {
                'color': '#00CC96'
            },
            "hovertemplate": "Week of %{x} <br>J&J Doses: %{y}"
        })
    )

fig.update_layout(vaccine_layout)

logger.info("Publishing chart.")
plot_url = py.plot(fig, filename='First-Dose_Vaccine_Allocation_DC', fileopt='extend', auto_open=False)
logger.info(f"Chart published at {plot_url}.")
