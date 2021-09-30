from loader.ldaps import LDAPSLoader
from preprocessor.preprocess import LDAPSPreprocessor
import pytz
import datetime
from scipy.constants import convert_temperature
import pvlib
import pandas as pd
import numpy as np

if __name__ == "__main__":
    kst = pytz.timezone("Asia/Seoul")  # Change timezone you want
    dt = datetime.datetime.now().replace(minute=0, second=0, microsecond=0)  # Change datetime you want
    start_dt = kst.localize(dt)  # Change datetime you want
    end_dt = kst.localize(dt + datetime.timedelta(days=1))  # Change datetime you want
    lat, lon = 37.4845, 127.0340

    loader = LDAPSLoader()
    # loader.collect_data(start_dt)
    df = loader(lat, lon, start_dt)

    df.temp = df.temp - 273.15
    df.dewpoint = df.dewpoint - 273.15
    print(df)

    ## pvlib
    df_min = df.resample('T').interpolate()
    for col in ['ghi','swdir','dhi']:
        df_min[col] = df_min[col].shift(-30)

    location = pvlib.location.Location(latitude=lat, longitude=lon, tz='Asia/Seoul', altitude=0)
    solpos = location.get_solarposition(df_min.index, pressure=df_min.pressure, temperature=df_min.temp)
    cs = location.get_clearsky(df_min.index, model='ineichen', solar_position=solpos, perez_enhancement=True)


    # cs_min2hour = cs_min.resample('h', label='right').mean()


    decomp_dni = pvlib.irradiance.dirindex(ghi=df_min.ghi,
                                           ghi_clearsky=cs.ghi,
                                           dni_clearsky=cs.dni,
                                           zenith=solpos.zenith,
                                           times=df_min.index,
                                           use_delta_kt_prime=True,
                                           pressure=df_min.pressure,
                                           temp_dew=df_min.dewpoint
                                           )

    df_min['decomp_dni'] = decomp_dni
    df_min['calc_dni'] = np.clip((df_min.ghi-df_min.dhi)/np.cos(np.radians(solpos.apparent_zenith)), 0, 2000)
    df_min['calc_dni'] = df_min['calc_dni'].where(solpos.zenith <= 87)



    df_min = df_min.resample('h', label='right').mean()
    df2 = df_min.copy()
    df2.calc_dni = df_min['calc_dni'].fillna(df2.swdir)

    # preproc_model = LDAPSPreprocessor(decomp_model="disc", clearsky_interpolate=True)
    # weather_preproc = preproc_model(
    #     lat,
    #     lon,
    #     altitude=0,
    #     weather=df,
    #     keep_solar_geometry=True,
    #     unstable_to_nan=True,
    # )
    # print(weather_preproc)
