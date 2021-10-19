import pandas as pd
import pytz
import datetime
import pvlib
import matplotlib.pyplot as plt
from loader.ldaps import LDAPSLoader
from preprocessor.preprocess import LDAPSPreprocessor
from functools import reduce
import requests


#############################
# LDAPS 모델 일사량 정확도 비교 코드
############################

def getKmaAsosWeather(start, end, stdId):
    key = 'x17Q2WlJi5iGJTaZmcWPiOkETcrPKMA0ivHmSSlbN2y1zaPrNWMtyd1vb236WBEyce0gKtNiF4HfqCFElrVRNQ%3D%3D'
    url = 'http://apis.data.go.kr/1360000/AsosHourlyInfoService/getWthrDataList'
    startDt = start[:8]
    startHh = start[8:]
    endDt = end[:8]
    endHh = end[8:]
    queryParams = f"?serviceKey={key}&pageNo=1&numOfRows=999&dataType=JSON&dataCd=ASOS&dateCd=HR" \
                  f"&startDt={startDt}&startHh={startHh}&endDt={endDt}&endHh={endHh}&stnIds={stdId}"
    try:
        response = requests.get(url + queryParams)

        data = response.json()
        df = pd.DataFrame.from_dict(data['response']['body']['items']['item'])
        df.tm = pd.to_datetime(df.tm)
        df.icsr = pd.to_numeric(df.icsr) * 1000000 / 3600
    except Exception as e:
        print(f'Error occurred during query: {url + queryParams}')
        print(response)
        print(e)
    finally:
        return df



if __name__ == "__main__":

    # asos = pd.read_csv("ASOS.csv", index_col='일시', encoding='cp949', parse_dates=True)
    # asos.index = asos.index.tz_localize('Asia/Seoul')
    # asos = asos.loc[asos.index[0]+pd.to_timedelta('12d'):]
    # asos.index.name = 'dt'
    # asos.columns = ['id', 'stn', 'asos_ghi']
    # asos.asos_ghi *= 1000000 / 3600
    time_range = pd.date_range(start='20211012 0300', end=pd.Timestamp.today(), tz='Asia/Seoul', freq='6h', closed='left')


    lst = []
    # for targ in pd.date_range(start=asos.index[0]+pd.to_timedelta('3h'), end=asos.index[-1], freq='6h'):
    for targ in time_range:
        print(f"Call Datetime                  {targ.strftime('%Y-%m-%d %H시 %Z')}")
        start_dt = targ
        # kst = pytz.timezone("Asia/Seoul")  # Change timezone you want
        # dt = datetime.datetime.now()  # Change datetime you want
        # start_dt = kst.localize(dt)  # Change datetime you want
        lat, lon = 34.6261,	126.7689
        try:
            loader = LDAPSLoader(data_root="/home/linuxenerdot/Desktop/LDPS_data/data")
            # loader.collect_data(start_dt)
            df = loader(lat, lon, start_dt, use_local_latest=True, exclude_col=['SWDIF','VLCDC','LCDC','MCDC','HCDC','TCAM'])

            preproc_model = LDAPSPreprocessor(decomp_model='dirint', clearsky_interpolate=True)
            weather_preproc = preproc_model(
                lat,
                lon,
                weather=df,
                keep_solar_geometry=True,
                unstable_to_nan=True,
            )

            weather_preproc['ghi'] = weather_preproc['ghi'].resample('h', closed='right', label='right').mean()
            weather_preproc = weather_preproc.tz_convert('Asia/Seoul')
            for col in weather_preproc:
                if col in ['ghi', 'dhi', 'dni', 'cs_ghi', 'cs_dhi', 'cs_dni']:
                    weather_preproc[col] = weather_preproc[col].resample('h', closed='right', label='right').mean()
            weather_preproc = weather_preproc.resample('h').asfreq()
            weather_preproc = weather_preproc.assign(call=targ).set_index('call', append=True)

            lst.append(weather_preproc.loc[targ.replace(hour=0, minute=0, second=0, microsecond=0)+ pd.to_timedelta('24h'):targ.replace(hour=0, minute=0, second=0, microsecond=0)+ pd.to_timedelta('47h')])
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
            # irr_df.index = irr_df.index.tz_convert('Asia/Seoul')
        except Exception as e:
            print(targ, e)

    fcst_df = reduce(lambda x, y: x.append(y), lst)
    asostest = getKmaAsosWeather(fcst_df.index.get_level_values('dt').min().strftime('%Y%m%d%H%M%S'),
                                 (pd.Timestamp.today().date()-pd.to_timedelta('1d')).strftime('%Y%m%d%H%M%S'),
                                 259)
    asostest.set_index('tm', inplace=True, drop=True)
    asostest.index = asostest.index.tz_localize('Asia/Seoul')
    start_lst = [asostest.index[0]]
    end_lst = [asostest.index[-1]]
    fig, ax = plt.subplots(4, 1, figsize=(12,17))
    for idx, (hour, call_df) in enumerate(fcst_df.groupby(fcst_df.index.get_level_values('call').hour)):
        call_df.index = call_df.index.droplevel('call')
        call_df.index = call_df.index.tz_convert('Asia/Seoul')

        data = asostest.merge(call_df, how='outer', left_index=True, right_index=True)
        data.index = pd.to_datetime(data.index).tz_convert('Asia/Seoul')

        # plot fcst result
        ax[idx].title.set_text(f'called @ {hour} KST\nsimulated @ -6 hours\nprediction next day')
        data[['icsr','ghi']].plot(ax=ax[idx])
        start_lst.append(call_df.index[0])
        end_lst.append(call_df.index[-1])
    for axi in ax:
        axi.set_xlim(min(start_lst), max(end_lst))
    plt.tight_layout()
    plt.show()