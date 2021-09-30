# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np

from pvlib import irradiance
from pvlib.location import Location


class BasePreprocessor(object):
    """BasePreprocessor
    this class transforms raw weather data to preprocessd one
    required to passed to ModelChain's input
    """

    __AVAILABLE_DECOMP_MODELS = ["disc", "dirint"]

    def __init__(self, decomp_model="dirint", **kwargs):
        """__init__

        Parameters
        ----------

        decomp_model : str
            - currently ['disc', 'dirint'] is available
        **kwargs :

        Returns
        -------
        """

        self._set_decomp_model(decomp_model)

    def _set_decomp_model(self, decomp_model):
        """_set_decomp_model

        Parameters
        ----------

        decomp_model : str
            - currently ['disc', 'dirint'] is available

        Returns
        -------
        """

        if decomp_model not in self.__AVAILABLE_DECOMP_MODELS:
            raise ValueError(
                "currently, only {} is available decomposition models".format(
                    self.__AVAILABLE_DECOMP_MODELS
                )
            )

        self._decomp_model = getattr(irradiance, decomp_model)

    @staticmethod
    def uv_to_speed(wind_speed_u, wind_speed_v):
        """uv_to_speed

        Parameters
        ----------

        wind_speed_u : pd.Series
        wind_speed_v : pd.Series

        Returns
        -------
        """

        wind_speed = np.sqrt(wind_speed_u ** 2 + wind_speed_v ** 2)
        wind_speed.name = "wind_speed"

        return wind_speed

    @staticmethod
    def kelvin_to_celsius(temp_air):
        """kelvin_to_celsius

        Parameters
        ----------

        temp_air : pd.Series

        Returns
        -------
        """

        temp_air = temp_air - 273.15
        temp_air.name = "temp_air"

        return temp_air

    @staticmethod
    def get_clearsky_index(ghi, cs_ghi, csi_min=0, csi_max=1.2, eps=1e-6):
        """get_clearsky_index

        Parameters
        ----------

        ghi : pd.Series
        cs_ghi : pd.Series
        csi_min : float
        csi_max : float
        eps : float

        Returns
        -------
        """

        assert len(ghi) == len(cs_ghi)

        ghi = np.array(ghi)
        cs_ghi = np.array(cs_ghi)
        csi = np.clip(np.divide(ghi, cs_ghi + eps), csi_min, csi_max)

        return csi

    # pvlib.forecast.py's default method is 'clearsky_scaling'
    @staticmethod
    def decompose(ghi, solar_zenith, model):
        """decompose

        Parameters
        ----------

        ghi : pd.Series
        solar_zenith : pd.Series
        model : function
            - decomposition model defined in pvlib.irradiance

        Returns
        -------
        """

        dni = model(ghi, solar_zenith, ghi.index)
        # NOTE: disc returns dataframe containg dni as a column value
        if "dni" in dni:
            dni = dni["dni"]
        dhi = ghi - dni * np.cos(np.radians(solar_zenith))

        irrads = pd.DataFrame({"ghi": ghi, "dni": dni, "dhi": dhi})
        # NOTE: set missing value as 0 only if ghi value is missed
        # this step is required since implementation of disc and dirint model
        # in pvlib automatically replaces nans to 0
        irrads.loc[~irrads.ghi.isnull() & irrads.dni.isnull(), "dni"] = 0.0
        irrads.loc[~irrads.ghi.isnull() & irrads.dhi.isnull(), "dhi"] = 0.0

        return irrads

    @staticmethod
    def flag_interp_stability(raw_data, processed_data):

        stability = pd.DataFrame(index=processed_data.index)
        stability.loc[raw_data.dropna().index, "stable"] = True
        stable_idx = stability.stable.ffill() & stability.stable.bfill()
        stability.loc[stable_idx, "stable"] = True
        stability.loc[~stable_idx, "stable"] = False
        stability["stable"] = stability.stable.astype(bool).values

        processed_data = processed_data.copy()
        processed_data = processed_data.join(stability)

        return processed_data

    # implement this at the subclasss
    def preprocess(self):

        raise NotImplementedError

    # implement this at the subclasss
    def __call__(self):

        raise NotImplementedError


class LDAPSPreprocessor(BasePreprocessor):

    __REQUIRED_COLUMNS = ["temp_air", "ghi", "wind_speed_u", "wind_speed_v"]

    def __init__(self, decomp_model="dirint", clearsky_interpolate=True, **kwargs):
        """__init__

        Parameters
        ----------

        decomp_model : str
            - currently ['disc', 'dirint'] is available
        clearsky_interpolate : bool
            - apply csi interpolation if given
        **kwargs :

        Returns
        -------
        """

        super(LDAPSPreprocessor, self).__init__(decomp_model=decomp_model, **kwargs)
        self.clearsky_interpolate = clearsky_interpolate

    def _check_sanity(self, data):
        """_check_sanity

        Parameters
        ----------

        data : pd.DataFrame

        Returns
        -------
        """

        assert "dt" in data.index.names

        cols = set(data.columns)
        if not set(self.__REQUIRED_COLUMNS) <= cols:
            raise KeyError("one of {} does not exists".format(self.__REQUIRED_COLUMNS))

    def preprocess(self, weather):
        """preprocess

        Parameters
        ----------

        weather : pd.DataFrame

        Returns
        -------
        """

        temp_air = self.kelvin_to_celsius(weather["temp_air"])
        wind_speed = self.uv_to_speed(weather["wind_speed_u"], weather["wind_speed_v"])
        weather_preproc = pd.DataFrame({"temp_air": temp_air, "wind_speed": wind_speed})

        return weather_preproc

    def __call__(
        self,
        latitude,
        longitude,
        altitude,
        weather,
        keep_solar_geometry=True,
        unstable_to_nan=True,
    ):
        """__call__

        Parameters
        ----------

        latitude : float
        longitude : float
        altitude : float
        weather : pd.DataFrame
        keep_solar_geometry : bool
            - include solar geometry features to output data if True
        unstable_to_nan : bool
            - replace unstable points to nan if True

        Returns
        -------
        """

        self._check_sanity(weather)

        raw_data = weather.copy()
        weather = weather.resample("1h").asfreq()

        location = Location(
            latitude, longitude, altitude=altitude, tz=weather.index.tz.zone
        )
        solpos = location.get_solarposition(weather.index)
        weather = weather.join(solpos)

        if self.clearsky_interpolate:
            cs_irrads = location.get_clearsky(weather.index, solar_position=solpos)
            cs_irrads.columns = [f"cs_{c}" for c in cs_irrads.columns]
            cs_irrads.index.name = "dt"
            weather = weather.join(cs_irrads)
            csi = self.get_clearsky_index(weather.ghi, weather.cs_ghi)
            weather["csi"] = csi
            weather = weather.interpolate()
            weather["ghi"] = np.array(weather.cs_ghi) * np.array(weather.csi)
        else:
            weather = weather.interpolate()

        # TODO: remove run_dt
        # based on the number of missing points in raw weather data
        irrads = self.decompose(weather["ghi"], weather["zenith"], self._decomp_model)
        weather_preproc = self.preprocess(weather)
        weather_preproc = weather_preproc.join(irrads)
        basic_feature_cols = weather_preproc.columns
        if keep_solar_geometry:
            weather_preproc = weather_preproc.join(solpos)
            if self.clearsky_interpolate:
                weather_preproc = weather_preproc.join(cs_irrads)

        weather_preproc = self.flag_interp_stability(raw_data, weather_preproc)
        if unstable_to_nan:
            weather_preproc.loc[~weather_preproc.stable, basic_feature_cols] = np.nan

        return weather_preproc


if __name__ == "__main__":
    from loader.ldaps import LDAPSLoader
    import pytz
    import datetime

    kst = pytz.timezone("Asia/Seoul")
    dt = datetime.datetime.now().replace(minute=0, second=0, microsecond=0)  # Change datetime you want
    start_dt = kst.localize(dt)  # Change datetime you want
    end_dt = kst.localize(dt + datetime.timedelta(days=1))  # Change datetime you want
    lat, lon = 37.123, 126.598
    # ins.latest_simulation(start_dt, end_dt, verbose=True)
    loader = LDAPSLoader()
    # loader.collect_data(start_dt, end_dt)
    data = loader(lat, lon, start_dt)

    preproc_model = LDAPSPreprocessor(decomp_model="disc", clearsky_interpolate=True)
    weather_preproc = preproc_model(
        lat,
        lon,
        altitude=0,
        weather=data,
        keep_solar_geometry=True,
        unstable_to_nan=True,
    )
    print(weather_preproc)
