import datetime
import json
import logging
import sys
from contextlib import redirect_stdout
from logging import config

import pytz

from dbIntegration.dbUplaodPlantFcst import updateDB, getTargPlant

##############################
# 일 4회 LDAPS 산출물 다운로드
# crontab 참고
##############################


if __name__ == "__main__":
    with open("/home/LDAPS/loggerSettings.json", "rt") as file:
        config = json.load(file)

    logging.config.dictConfig(config)
    logger = logging.getLogger()
    logging.write = lambda msg: logging.info(msg) if msg != '\n' else None
    logging.flush = sys.stdout.flush


    from loader.ldaps import LDAPSLoader

    with redirect_stdout(logging):
        kst = pytz.timezone("Asia/Seoul")  # Change timezone you want
        dt = datetime.datetime.now()  # Change datetime you want
        start_dt = kst.localize(dt)  # Change datetime you want
        # end_dt = kst.localize(dt + datetime.timedelta(days=1))  # Change datetime you want

        loader = LDAPSLoader(data_root="/home/LDAPS/data")
        latest_simul_dt = loader.collect_data(start_dt)

        targ_plant = getTargPlant(['LDS', 'razzler'])
        for idx, row in targ_plant.iterrows():
            df = loader(row.latitude, row.longitude, start_dt)
            df['plant_id'] = row.plant_id
            df['ctime'] = df.index.tz_convert(pytz.timezone('Asia/Seoul')).strftime('%H:%M:%S')
            df['cdate'] = df.index.tz_convert(pytz.timezone('Asia/Seoul')).strftime('%Y-%m-%d')
            df['base_time'] = (latest_simul_dt + datetime.timedelta(hours=6)).astimezone(
                pytz.timezone('Asia/Seoul')).strftime('%H:%M:%S')
            updateDB(df)
        loader.clean()
        #
        #
        # preproc_model = LDAPSPreprocessor(decomp_model='dirint', clearsky_interpolate=True)
        # weather_preproc = preproc_model(
        #     lat,
        #     lon,
        #     weather=df,
        #     keep_solar_geometry=True,
        #     unstable_to_nan=True,
        # )
        #
        # irr_df = pvlib.irradiance.get_total_irradiance(
        #     surface_tilt=lat,
        #     surface_azimuth=180,
        #     solar_zenith=weather_preproc['apparent_zenith'],
        #     solar_azimuth=weather_preproc['azimuth'],
        #     ghi=weather_preproc['ghi'],
        #     dni=weather_preproc['dni'],
        #     dhi=weather_preproc['dhi'],
        #     model='perez',
        #     dni_extra=pvlib.irradiance.get_extra_radiation(weather_preproc.index))
        # print()
        # irr_df = irr_df.resample('h', closed='right', label='right').mean()
        # for col in weather_preproc:
        #     if col in ['ghi', 'dhi', 'dni', 'cs_ghi', 'cs_dhi', 'cs_dni']:
        #         irr_df[col] = weather_preproc[col].resample('h', closed='right', label='right').mean()
        #     else:
        #         irr_df[col] = weather_preproc[col].resample('h').asfreq()
        #
        #
        # irr_df.index = irr_df.index.tz_convert('Asia/Seoul')
