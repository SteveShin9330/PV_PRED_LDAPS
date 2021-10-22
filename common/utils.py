# -*- coding: utf-8 -*-

import math
import os
from datetime import datetime

import pandas as pd


def build_kwargs(params, **kwargs):
    """build_kwargs
    update params dictionary by iterating over kwargs.items()

    Parameters
    ----------

    params : dict
    **kwargs : dict

    Returns
    -------
    """

    for k, v in kwargs.items():
        if k in params:
            params[k] = v

    return params


def watts_to_energy(watts, unit="kwh"):
    """watts_to_energy

    Parameters
    ----------

    watts : pd.Series
    unit : str

    Returns
    -------
    """

    conv_dict = {
        "w": lambda x: x,
        "kwh": lambda x: x.rolling("1h", closed="both").mean() / 1000.0,
        "mj": lambda x: x * 3600.0 / 10 ** 6,
    }

    unit = unit.lower()
    formula = conv_dict.get(unit)

    if formula is None:
        raise ValueError(
            "pass one of {} as unit parameter".format(list(conv_dict.keys()))
        )

    energy = formula(watts)

    return energy


def custom_round(x):
    """custom_round
    see below for more details
    - https://stackoverflow.com/questions/10825926/python-3-x-rounding-behavior

    Parameters
    ----------

    x : float

    Returns
    -------
    """

    f = math.floor(x)
    val = x if x - f < 0.5 else f + 1

    return int(val)


def str_datetime_validate(str_datetime: str, _format: str):
    try:
        return datetime.strptime(str_datetime, _format)
    except ValueError:
        raise Exception


def make_dir(path):
    if not os.path.isdir(path):
        os.makedirs(path)


def read_ldaps_day_ahead_inputs(start_date, end_date, run_before="15h", tz="Asia/Seoul"):
    """read_gfs_day_ahead_inputs

    Parameters
    ----------

    start_date : str
        - yyyy-mm-dd
    end_date : str
        - yyyy-mm-dd
    run_before : str
        - difference between bidding time and forecast start time in hours
    tz : str

    Returns
    -------
    """

    try:
        delay = int(run_before.replace("h", ""))
    except Exception:
        raise ValueError("pass run_before like ^[0-9]{1,}h$")

    fct_dts = pd.date_range(start_date, end_date, tz=tz)
    run_dts = (fct_dts - pd.Timedelta(hours=delay)).tolist()
    fct_dts = [fct_dts[0]] + (fct_dts[1:] + pd.Timedelta(hours=3)).tolist()
    # day 1: 21, 0, 3, ....., 21 -> read 9 files
    # day 2: 0, 3, ...., 21 -> read 8 files
    # day 3: 0, 3, ...., 21 -> read 8 files
    # day N: 0, 3, ...., 21, 0 -> read 9 files
    fct_ranges = [21] + [18 for i in range(len(fct_dts) - 2)] + [21]

    return list(zip(run_dts, fct_dts, fct_ranges))


if __name__ == "__main__":
    r = read_ldaps_day_ahead_inputs("2021-09-01", "2021-09-02", "15h", "UTC")
    print(r)
    print(len(r))
    print(r[0])
