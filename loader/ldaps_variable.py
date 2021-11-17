# -*- coding: utf-8 -*-

from collections import OrderedDict

LDAPS_SOLAR_DEFAULT = {
    "TDSWS": 'ghi',
    "SWDIF": 'dhi',
    "TMP": 'temp_air',
    "UGRD": 'wind_speed_u',
    "VGRD": 'wind_speed_v',
    "DPT": 'temp_dew',
    "PRE": 'pressure',
    "VLCDC": 'vlow_clouds',
    "LCDC": 'low_clouds',
    "MCDC": 'mid_clouds',
    "HCDC": 'high_clouds',
    "TCAM": 'total_clouds'
}

LDAPS_GRIB = OrderedDict(
    {
        "TDSWS": {
            "name": "Total Downward Sfc. SW Flux",
            "shortName": "tdsw",
            "typeOfLevel": "surface",
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
        "VLCDC": {
            "name": "Very Low Cloud Cover",
            "shortName": "vlcd",
            "typeOfLevel": "very low atmosphere",
        },
        "LCDC": {
            "name": "Low Cloud Cover",
            "shortName": "lcdc",
            "typeOfLevel": "low atmosphere",
        },
        "MCDC": {
            "name": "Medium Cloud Cover",
            "shortName": "mcdc",
            "typeOfLevel": "medium atmosphere",
        },
        "HCDC": {
            "name": "High Cloud Cover",
            "shortName": "hcdc",
            "typeOfLevel": "high atmosphere",
        },
        "TCAM": {
            "name": "Total Cloud Amount - Max/Rdm Overlp",
            "shortName": "tcam",
            "typeOfLevel": "total atmosphere",
        },
    }
)
