# -*- coding: utf-8 -*-

from collections import OrderedDict

LDAPS_SOLAR_DEFAULT = {
    "TDSWS": 'ghi',
    "SWDIR": 'swdir',
    "SWDIF": 'dhi',
    "TMP": 'temp',
    "UGRD": 'wind_speed_u',
    "VGRD": 'wind_speed_v',
    "DPT": 'dewpoint',
    "PRE": 'pressure'
}

LDAPS_GRIB = OrderedDict(
    {
        "TDSWS": {
            "name": "Total Downward Sfc. SW Flux",
            "shortName": "tdsw",
            "typeOfLevel": "surface",
        },
        "SWDIR": {
            "name": "Direct SW Flux(ON RHO LEVELS)",
            "shortName": "swdr",
            "typeOfLevel": "2m above ground",
        },
        "SWDIF": {
            "name": "Diffuse SW RAD Flux(ON RHO LEVELS)",
            "shortName": "swdf",
            "typeOfLevel": "2m above ground",
        },
        "TMP": {
            "name": "Temperature",
            "shortName": "tmps",
            "typeOfLevel": "surface",
        },
        "UGRD": {
            "name": "U-Component of wind",
            "shortName": "ugrd",
            "typeOfLevel": "10m above ground",
        },
        "VGRD": {
            "name": "V-Component of wind",
            "shortName": "vgrd",
            "typeOfLevel": "10m above ground",
        },
        "DPT": {
            "name": "Dewpoint Temperature",
            "shortName": "dptp",
            "typeOfLevel": "1.5m above ground",
        },
        "PRE": {
            "name": "Surface Pressure",
            "shortName": "pres",
            "typeOfLevel": "surface",
        },
    }
)
