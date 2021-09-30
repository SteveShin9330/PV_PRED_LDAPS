import json

import requests
import numpy as np
import arrow
import math
import datetime
import pytz
import pandas as pd
import numpy as np
import os
import xarray
from ftplib import FTP
from retrying import retry
from scipy.constants import convert_temperature

from tqdm import tqdm,trange
# from ..exceptions import (
#     DataRootNullException,
#     LatLonInvalidException,
#     EccodesRuntimeErrorException,
# )
from common.utils import custom_round, make_dir
from loader.ldaps_variable import LDAPS_GRIB, LDAPS_SOLAR_DEFAULT


class LDAPSLoader(object):
    def __init__(self, data_root="/tmp"):
        """__init__
        Parameters
        ----------
        data_root: str
        Returns
        -------
        """
        # if not data_root:
        #     raise DataRootNullException

        self.data_root = data_root
        self.HOST = 'ncms.kma.go.kr'
        self.PORT = 10021
        self.col_mapping = list(LDAPS_GRIB.keys())
        self.simulation_time = None



    @retry(stop_max_attempt_number=400, wait_random_max=20)
    def refresh_simulation_time(self) -> list:
        """__init__
        Parameters
        ----------
        Returns
        -------
        simulation times: list
        """

        try:
            ftp = FTP()
            ftp.connect(self.HOST, self.PORT)
            ftp.login()
            ftp.cwd('LDPS/UNIS')
            fn_lst = ftp.nlst()

            dts_lst = list(
                set([
                    datetime.datetime.strptime(x.split('.')[-2], "%Y%m%d%H").replace(tzinfo=pytz.timezone("UTC"))
                    for x in tqdm(fn_lst)
                ])
            )
            for idx, dta in enumerate(dts_lst):
                for grib_idx, (key, value) in enumerate(LDAPS_GRIB.items()):
                    cnt = pd.Series(fn_lst).str.contains(rf'^(?=.*{dta.strftime("%Y%m%d%H")})(?=.*{value["shortName"]})').sum()
                    if cnt !=49:
                        dts_lst.pop(idx)
                        break
            dts_lst.sort()
            self.simulation_time = dts_lst
        finally:
            ftp.close()
        return dts_lst

    def latest_simulation(
        self,
        fct_start_dt: datetime.datetime,
        refresh=False,
        verbose=False,
    ) -> datetime.datetime:
        if refresh or self.simulation_time is None:
            print("Refresh Available Simulation Time")
            print("=================================")
            self.refresh_simulation_time()
        st = fct_start_dt.astimezone(pytz.timezone("UTC"))
        dt_diff = [(st - dt).total_seconds() for dt in self.simulation_time]
        if verbose:
            print(st)
            for idx, val in enumerate(self.simulation_time):
                print(val, dt_diff[idx])
        idx = np.argmin(np.array(dt_diff))
        return self.simulation_time[idx]

    @retry(stop_max_attempt_number=400, wait_random_max=20)
    def download_data(self, fn):
        try:
            HOST = 'ncms.kma.go.kr'
            PORT = 10021
            ftp = FTP()
            res = ftp.connect(HOST, PORT)
            res = ftp.login()
            res = ftp.cwd('LDPS/UNIS')
            with open(os.open(os.path.join('/tmp', fn), os.O_CREAT | os.O_WRONLY, 0o777), "wb") as f:
                ftp.retrbinary('RETR %s' % fn, f.write)
        finally:
            ftp.close()
        return 0

    def collect_data(self, start_dt: datetime.datetime):
        latest_simul_dt = self.latest_simulation(start_dt)
        print(f"Latest LDAPS Simulation Datetime: {latest_simul_dt}")

        print("Download UM-LDAPS 1.5km L70 Data")
        print("====================================")


        for hour in tqdm(range(49)):
            simul_dt = latest_simul_dt.strftime("%Y%m%d%H")
            for grib_idx, (key, value) in enumerate(LDAPS_GRIB.items()):
                fn = f'l015_v070_{value["shortName"]}_unis_h{hour:03d}.{simul_dt}.gb2'
                self.download_data(fn)


    def __call__(self, lat, lon, fct_start_dt):


        latest_simul_dt = self.latest_simulation(fct_start_dt)

        index_lst = pd.date_range(latest_simul_dt,
                                  latest_simul_dt + datetime.timedelta(hours=48),
                                  freq='h', tz='UTC')
        df = pd.DataFrame(index=index_lst)

        print(f"Extracting UM-LDAPS 1.5km Data")
        print("====================================")


        for grib_idx, (key, value) in tqdm(enumerate(LDAPS_GRIB.items())):
            col_lst = []
            for hour in trange(49, desc=f'{value["name"]:35}', position=0):
                simul_dt = latest_simul_dt.strftime("%Y%m%d%H")

                fn = f'l015_v070_{value["shortName"]}_unis_h{hour:03d}.{simul_dt}.gb2'
                # cmd = f'../kwgrib2/bin/kwgrib2 /tmp/{fn}'
                # os.system(cmd)
                # cmd = f'../kwgrib2/bin/kwgrib2 /tmp/{fn} -var -ftime'
                # os.system(cmd)
                cmd = f'kwgrib2/bin/kwgrib2 /tmp/{fn} -lon {lon} {lat}'
                data = os.popen(cmd).read()
                data = data.split(':')[2:]
                data[-1] = data[-1][:-1]

                val = float(data[-1].split('=')[-1])
                col_lst.append(val)

            df[key] = col_lst

        df.index.name='dt'
        df.columns = df.columns.map(LDAPS_SOLAR_DEFAULT)
        return df


        #
        # dt_index, array = self.read_gfs(fct_start_dt, fct_end_dt)
        # data_array = self._process_latlon(array, lat, lon)
        # weather_data = pd.DataFrame(data_array.reshape(-1, 4), index=dt_index)
        # weather_data.columns = self.col_mapping
        # weather_data.index.name = "dt"
        # return weather_data

if __name__ == "__main__":
    kst = pytz.timezone("Asia/Seoul")  # Change timezone you want
    dt = datetime.datetime.now().replace(minute=0, second=0, microsecond=0)  # Change datetime you want
    start_dt = kst.localize(dt)  # Change datetime you want
    end_dt = kst.localize(dt + datetime.timedelta(days=1))  # Change datetime you want
    lat, lon = 37.123, 126.598

    loader = LDAPSLoader()
    # loader.collect_data(start_dt)
    df = loader(lat, lon, start_dt)
    print(df)
