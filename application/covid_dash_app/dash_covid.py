"""Create a Dash app within a Flask app."""
from pathlib import Path
import dash
from dash.dependencies import Input, Output
import dash_table
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objects as go
import pandas as pd
from .layout import html_layout
from .etl import get_datasets, init_cache, Constants

cache_config = {
    "DEBUG": True,
    "CACHE_TYPE": "filesystem",
    "CACHE_DIR": "cache-directory",
    "CACHE_THRESHOLD": 10,
}


def Add_Dash(server):
    """Create a Dash app."""
    external_stylesheets = [
        "https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/css/bootstrap.min.css",
        "https://fonts.googleapis.com/css?family=Lato",
        "https://use.fontawesome.com/releases/v5.8.1/css/all.css",
    ]
    external_scripts = [
        "https://code.jquery.com/jquery-3.2.1.slim.min.js",
        "https://cdnjs.cloudflare.com/ajax/libs/popper.js/1.12.9/umd/popper.min.js",
        "https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/js/bootstrap.min.js",
        "/static/dist/js/main.js",
    ]
    dash_app = dash.Dash(
        server=server,
        external_stylesheets=external_stylesheets,
        external_scripts=external_scripts,
        routes_pathname_prefix="/covid19/",
    )

    server.config.from_mapping(cache_config)
    init_cache(server)

    # Override the underlying HTML template
    dash_app.index_string = html_layout

    dash_app.layout = generate_layout

    @dash_app.callback(Output("signal", "children"), [Input("map_type", "value")])
    def filter_dataset(value):
        datasets, last_update_dt = get_datasets()
        df = datasets[Constants.TS_COVID19_CONSOLIDATED_COUNTRY]
        df = df.reset_index()

        # Get the max date from full confirmed dataset
        max_date = df["Date"].max()

        # Get dataset for day of max date
        df_max_date = df[df["Date"] == max_date]

        return [value, df_max_date.to_json()]

    @dash_app.callback(Output("map", "children"), [Input("signal", "children")])
    def update_map(children):
        colname = "Total %s" % key_to_colname(children[0])
        return total_cases_world_map(pd.read_json(children[1]), colname)

    @dash_app.callback(
        Output("totals_table", "children"), [Input("signal", "children")]
    )
    def update_table(children):
        colname = "Total %s" % key_to_colname(children[0])
        row = pd.DataFrame(
            {
                "Country/Region": "Global",
                "Date": "",
                colname: pd.read_json(children[1])[colname].sum(),
            },
            index=[-1],
        )
        df = pd.concat([row, pd.read_json(children[1])])
        return total_cases_by_country_table(df, colname)

    return dash_app.server


def key_to_colname(key):
    if "confirmed" in key:
        return "Confirmed"
    elif "deaths" in key:
        return "Deaths"
    elif "recovered" in key:
        return "Recoveries"
    elif "active" in key:
        return "Active Cases"
    return "Unknown"


def generate_layout():

    datasets, last_update_dt = get_datasets()

    # Create Dash Layout
    return html.Div(
        children=[
            html.Div(
                className="container",
                children=[
                    html.Div(
                        className="row",
                        children=[
                            html.H1("COVID-19 Global Situation"),
                            dcc.Dropdown(
                                id="map_type",
                                className="dropdown",
                                style={"width": "100%"},
                                options=[
                                    {
                                        "label": "Confirmed Cases by Country/Region",
                                        "value": Constants.TS_COVID19_CONFIRMED_COUNTRY,
                                    },
                                    {
                                        "label": "Deaths by Country/Region",
                                        "value": Constants.TS_COVID19_DEATHS_COUNTRY,
                                    },
                                    {
                                        "label": "Active Cases by Country/Region",
                                        "value": Constants.TS_COVID19_ACTIVE_CASES_COUNTRY,
                                    },
                                ],
                                value=Constants.TS_COVID19_CONFIRMED_COUNTRY,
                            ),
                        ],
                    ),
                    html.Div(
                        className="row",
                        children=[
                            html.Div(
                                className="col-sm-6 col-lg-3",
                                children=[
                                    html.Div(
                                        className="row",
                                        children=[html.Div(id="totals_table"),],
                                    ),
                                ],
                            ),
                            html.Div(
                                className="col",
                                children=[
                                    html.Div(className="row", children=[],),
                                    html.Div(
                                        className="row",
                                        children=[html.Div(id="map", className="col"),],
                                    ),
                                ],
                            ),
                        ],
                    ),
                    html.Div(
                        className="row",
                        children=[
                            html.Div(
                                className="col-sm-6 col-lg-3",
                                children=generate_card(
                                    "Last Updated at", last_update_dt
                                ),
                            ),
                            html.Div(
                                className="col",
                                children=dcc.Markdown(
                                    """Data source: [John Hopkins University CSSE](https://github.com/CSSEGISandData/COVID-19)  
Confirmed cases include presumptive positive cases.  
Recovered cases outside China are estimates based on local media reports, and may be substantially lower than the true number.  
Active cases = total confirmed - total recovered - total deaths.
"""
                                ),
                            ),
                        ],
                    ),
                    html.Div(id="signal", style={"display": "none"}),
                ],
            )
        ],
        id="dash-container",
    )


def total_cases_by_country_table(df, colname="Total Cases"):
    df = df.sort_values(colname, ascending=False)
    dt_columns = [colname, "Country/Region"]
    table = table_from_dataframe(
        df[dt_columns],
        max_rows=500,
        headers=False,
        table_style={
            "overflow": "auto",
            "display": "block",
            "position": "relative",
            "height": "300px",
        },
    )
    return [html.H5("%s by Country/Region" % colname), table]


def total_cases_world_map(df, colname="Total Cases"):
    """Create world map of confirmed cases by country/region."""
    scale = 100
    fig = go.Figure()

    df = df.fillna("")
    # df["text"] = colname + " " + (df[colname]).astype(str)
    df["text"] = (
        "Confirmed "
        + (df["Total Confirmed"]).astype(str)
        + "<br>Deaths "
        + (df["Total Deaths"]).astype(str)
        + "<br>Recovered "
        + (df["Total Recovered"]).astype(str)
        + "<br>Death to Cases Ratio "
        + (df["Death to Cases Ratio"]).round(3).astype(str)
        
    )
    fig.add_trace(
        go.Scattergeo(
            locationmode="country names",
            locations=df["Country/Region"],
            # lon = df['Long'],
            # lat = df['Lat'],
            text=df["text"],
            marker=dict(
                size=df[colname] / scale,
                color="red",
                line_color="rgb(40,40,40)",
                line_width=0.5,
                sizemode="area",
            ),
            name="Legend name goes here",
        )
    )

    fig.update_layout(
        autosize=True,
        showlegend=False,
        geo=dict(scope="world", landcolor="rgb(217, 217, 217)",),
        margin=dict(l=5, r=10, b=20, t=10, pad=0),
    )

    return [dcc.Graph(figure=fig)]


def table_from_dataframe(dataframe, max_rows=10, headers=True, table_style={}):
    table_rows = []
    if headers:
        table_rows.append(
            html.Thead(html.Tr([html.Th(col) for col in dataframe.columns]))
        )
    table_rows.append(
        html.Tbody(
            children=[
                html.Tr([html.Td(dataframe.iloc[i][col]) for col in dataframe.columns])
                for i in range(min(len(dataframe), max_rows))
            ],
            style=table_style,
        )
    )
    return html.Table(className="table table-sm", children=table_rows)


def generate_card(title, text):
    card = html.Div(
        className="card text-center",
        children=[
            html.Div(
                className="card-body",
                children=[
                    html.P(className="card-title", children=title),
                    html.P(className="class-text", children=text),
                ],
            )
        ],
    )
    return card


def generate_list(items):
    return html.Ul(
        className="list-group",
        children=[html.Li(i, className="list-group-item") for i in items],
    )
