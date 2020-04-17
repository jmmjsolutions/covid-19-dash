import urllib.request
import json
import pandas as pd
import numpy as np
import flask
from flask_caching import Cache


class Constants:
    TS_COVID19_CONFIRMED_GLOBAL = "time_series_covid19_confirmed_global"
    TS_COVID19_DEATHS_GLOBAL = "time_series_covid19_deaths_global"
    TS_COVID19_RECOVERED_GLOBAL = "time_series_covid19_recovered_global"
    TS_COVID19_CONFIRMED_COUNTRY = "time_series_covid19_confirmed_country"
    TS_COVID19_DEATHS_COUNTRY = "time_series_covid19_deaths_country"
    TS_COVID19_RECOVERED_COUNTRY = "time_series_covid19_recovered_country"
    TS_COVID19_DAILY_NEW_CONFIRMED_COUNTRY = (
        "time_series_covid19_daily_new_confirmed_country"
    )
    TS_COVID19_DAILY_NEW_DEATHS_COUNTRY = "time_series_covid19_daily_new_deaths_country"
    TS_COVID19_DAILY_NEW_RECOVERED_COUNTRY = (
        "time_series_covid19_daily_new_recovered_country"
    )
    TS_COVID19_CONSOLIDATED_COUNTRY = "time_series_covid19_consolidated_country"
    TS_COVID19_ACTIVE_CASES_COUNTRY = "time_series_covid19_active_cases_country"


cache = Cache()


def init_cache(app):
    cache.init_app(app)


# Date & time of last update to remote data
_last_src_data_update = "N/A"


@cache.memoize(timeout=360)
def get_datasets():
    """Return all loaded datasets as cleaned dataframes."""
    cssegis_data = [
        (
            Constants.TS_COVID19_CONFIRMED_GLOBAL,
            "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv",
        ),
        (
            Constants.TS_COVID19_DEATHS_GLOBAL,
            "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv",
        ),
        (
            Constants.TS_COVID19_RECOVERED_GLOBAL,
            "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv",
        ),
    ]

    dfs_global = {}

    for data in cssegis_data:
        data_id = data[0]
        data_url = data[1]
        print("Load %s...", (data_url,))
        df_raw = pd.read_csv(data_url)
        df_clean = clean_data(df_raw)
        print("Clean %s...", (data_url,))
        dfs_global[data_id] = df_clean

    dfs_country = {}
    for df_key in dfs_global.keys():
        dfs_country[df_key.replace("global", "country")] = country_data(
            dfs_global[df_key], "Cases", "Total %s" % key_to_case_type(df_key)
        )

    dfs_daily = {}
    for df_key in dfs_country.keys():
        case_type = key_to_case_type(df_key)
        old_colname = "Total %s" % case_type
        new_colname = "Daily New %s" % case_type
        dfs_daily[df_key.replace("_covid19_", "_covid19_daily_new_")] = daily_data(
            dfs_country[df_key], old_colname, new_colname
        )

    df_country_consol = pd.merge(
        dfs_country[Constants.TS_COVID19_CONFIRMED_COUNTRY],
        dfs_daily[Constants.TS_COVID19_DAILY_NEW_CONFIRMED_COUNTRY],
        how="left",
        left_index=True,
        right_index=True,
    )
    df_country_consol = pd.merge(
        df_country_consol,
        dfs_daily[Constants.TS_COVID19_DAILY_NEW_DEATHS_COUNTRY],
        how="left",
        left_index=True,
        right_index=True,
    )
    df_country_consol = pd.merge(
        df_country_consol,
        dfs_country[Constants.TS_COVID19_DEATHS_COUNTRY],
        how="left",
        left_index=True,
        right_index=True,
    )
    df_country_consol = pd.merge(
        df_country_consol,
        dfs_country[Constants.TS_COVID19_RECOVERED_COUNTRY],
        how="left",
        left_index=True,
        right_index=True,
    )
    df_country_consol = pd.merge(
        df_country_consol,
        dfs_daily[Constants.TS_COVID19_DAILY_NEW_RECOVERED_COUNTRY],
        how="left",
        left_index=True,
        right_index=True,
    )
    df_country_consol["Total Active Cases"] = (
        df_country_consol["Total Confirmed"]
        - df_country_consol["Total Deaths"]
        - df_country_consol["Total Recovered"]
    )
    df_country_consol["Share of Recovered - Closed Cases"] = np.round(
        df_country_consol["Total Recovered"]
        / (df_country_consol["Total Recovered"] + df_country_consol["Total Deaths"]),
        2,
    )
    df_country_consol["Death to Cases Ratio"] = np.round(
        df_country_consol["Total Deaths"] / df_country_consol["Total Confirmed"], 3
    )

    dfs_country_consol = {
        Constants.TS_COVID19_ACTIVE_CASES_COUNTRY: df_country_consol,
        Constants.TS_COVID19_CONSOLIDATED_COUNTRY: df_country_consol,
    }

    _last_src_data_update = _get_last_commit_date(
        "https://api.github.com/repos/CSSEGISandData/COVID-19/commits"
    )

    return (
        {**dfs_global, **dfs_country, **dfs_daily, **dfs_country_consol},
        _last_src_data_update,
    )


def clean_data(df_raw):
    df_cleaned = df_raw.melt(
        id_vars=["Province/State", "Country/Region", "Lat", "Long"],
        value_name="Cases",
        var_name="Date",
    )
    return df_cleaned


def key_to_case_type(key):
    if "confirmed" in key:
        return "Confirmed"
    elif "deaths" in key:
        return "Deaths"
    elif "recovered" in key:
        return "Recovered"
    return "Unknown"


def country_data(df_cleaned, oldname, newname):
    df_country = (
        df_cleaned.groupby(["Country/Region", "Date"])["Cases"].sum().reset_index()
    )
    df_country = df_country.set_index(["Country/Region", "Date"])
    df_country.index = df_country.index.set_levels(
        [df_country.index.levels[0], pd.to_datetime(df_country.index.levels[1])]
    )
    df_country = df_country.sort_values(["Country/Region", "Date"], ascending=True)
    df_country = df_country.rename(columns={oldname: newname})
    return df_country


### Get Daily Data from Cumulative sum
def daily_data(dfcountry, oldname, newname):
    dfcountrydaily = dfcountry.groupby(level=0).diff().fillna(0)
    dfcountrydaily = dfcountrydaily.rename(columns={oldname: newname})
    return dfcountrydaily


def _get_last_commit_date(url):
    """Returns date & time of last commit"""
    req = urllib.request.Request(url)

    r = urllib.request.urlopen(req).read()
    commit = json.loads(r.decode("utf-8"))
    raw_date = commit[0]["commit"]["committer"]["date"]

    return raw_date
