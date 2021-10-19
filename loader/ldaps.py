import datetime
import multiprocessing
import time

import pytz
import pandas as pd
import numpy as np
import os
from ftplib import FTP
from retrying import retry
import tqdm
import parmap
import subprocess

from tqdm.contrib.concurrent import process_map, thread_map
from multiprocessing import Manager

import glob
from itertools import repeat
from functools import partial

from tqdm import tqdm, trange
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

    @retry(stop_max_attempt_number=300, wait_random_max=100)
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
                    for x in fn_lst
                ])
            )
            for idx, dta in enumerate(dts_lst):
                for grib_idx, (key, value) in enumerate(LDAPS_GRIB.items()):
                    cnt = pd.Series(fn_lst).str.contains(
                        rf'^(?=.*{dta.strftime("%Y%m%d%H")})(?=.*{value["shortName"]})').sum()
                    if cnt != 49:
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
            use_local_latest=False,
            refresh=False,
            verbose=False
    ) -> datetime.datetime:
        st = fct_start_dt.astimezone(pytz.timezone("UTC"))
        if use_local_latest:
            fn_lst = list(set([path.split('.')[-2] + 'UTC' for path in glob.glob(f"{self.data_root}/*")]))
            fn_lst.sort()
            dt_lst = [pytz.utc.localize(datetime.datetime.strptime(fn, '%Y%m%d%H%Z')) for fn in fn_lst]
            dt_diff = [(st - dt).total_seconds() for dt in dt_lst]

            if verbose:
                print(st)
                for idx, val in enumerate(dt_lst):
                    print(val, dt_diff[idx] / (60 * 60), 'hour')
            pos_idx = np.where(np.array(dt_diff) >= 6 * 60 * 60)
            idx = np.argmin(np.array(dt_diff)[pos_idx])

            return dt_lst[pos_idx[0][idx]]

        else:
            if refresh or self.simulation_time is None:
                print("Refresh Available Simulation Time")
                print("=================================")
                self.refresh_simulation_time()
            dt_diff = [(st - dt).total_seconds() for dt in self.simulation_time]
            if verbose:
                print(st)
                for idx, val in enumerate(self.simulation_time):
                    print(val, dt_diff[idx] / (60 * 60))
            pos_idx = np.where(np.array(dt_diff) > 0)
            idx = np.argmin(np.array(dt_diff)[pos_idx])

            return self.simulation_time[pos_idx[0][idx]]

    @retry(stop_max_attempt_number=300, wait_random_max=100)
    def download_data(self, fn):
        try:
            HOST = 'ncms.kma.go.kr'
            PORT = 10021
            ftp = FTP()
            res = ftp.connect(HOST, PORT)
            res = ftp.login()
            res = ftp.cwd('LDPS/UNIS')
            with open(os.open(os.path.join(self.data_root, fn), os.O_CREAT | os.O_WRONLY, 0o777), "wb") as f:
                ftp.retrbinary('RETR %s' % fn, f.write)
        finally:
            ftp.close()
        return 0

    def download_data_threaded(self, n_thread, fn_lst):
        r = process_map(self.download_data, fn_lst, max_workers=n_thread)
        return r

    def collect_data(self, start_dt: datetime.datetime):
        latest_simul_dt = self.latest_simulation(start_dt, verbose=True)
        print(
            f"Latest LDAPS Simulation Datetime: {latest_simul_dt.astimezone(pytz.timezone('Asia/Seoul')).strftime('%Y-%m-%d %H시 %Z')}")
        print("Download UM-LDAPS 1.5km L70 Data")
        print("====================================")

        fn_lst = []
        for hour in range(49):
            simul_dt = latest_simul_dt.strftime("%Y%m%d%H")
            for grib_idx, (key, value) in enumerate(LDAPS_GRIB.items()):
                fn = f'l015_v070_{value["shortName"]}_unis_h{hour:03d}.{simul_dt}.gb2'
                # self.download_data(fn)
                fn_lst.append(fn)
        self.download_data_threaded(10, fn_lst)

    def unit_read(self, stor, cmd):
        val = os.popen(cmd).read()
        val = float(val.split('=')[-1])
        key = str(cmd.split(' ')[1].split('/')[-1])
        stor[key] = val

    def read_data_threaded(self, cmd_lst, n_thread, var, d):
        process_map(partial(self.unit_read, d), cmd_lst, max_workers=n_thread, desc=var)
        # parmap.map(self.unit_read, cmd_lst, d, pm_pbar=True, pm_processes=n_thread, desc=var)
        return d

    def __call__(self, lat, lon, fct_start_dt, use_local_latest=False, exclude_col=[]):
        m = Manager()

        latest_simul_dt = self.latest_simulation(fct_start_dt, use_local_latest)
        index_lst = pd.date_range(latest_simul_dt,
                                  latest_simul_dt + datetime.timedelta(hours=48),
                                  freq='h', tz='UTC')
        df = pd.DataFrame(index=index_lst)
        print(
            f"Extracting UM-LDAPS 1.5km Data {latest_simul_dt.astimezone(pytz.timezone('Asia/Seoul')).strftime('%Y-%m-%d %H시 %Z')}")
        print("====================================")

        for grib_idx, (key, value) in enumerate(LDAPS_GRIB.items()):
            if key not in exclude_col:
                fn_lst = []
                # for hour in trange(49, initial=0, desc=f'{value["name"]:35}'):
                for hour in range(49):
                    simul_dt = latest_simul_dt.strftime("%Y%m%d%H")
                    fn = f'l015_v070_{value["shortName"]}_unis_h{hour:03d}.{simul_dt}.gb2'
                    fn_lst.append(f'./kwgrib2 {os.path.join(self.data_root, fn)} -lon {lon} {lat}')

                d = m.dict({str(i.split(' ')[1].split('/')[-1]): None for i in fn_lst})
                res = self.read_data_threaded(fn_lst, 49, f'{value["name"]:35}', d)

        #             # print(fn)
        #             # cmd = f'kwgrib2 {os.path.join(self.data_root, fn)}'
        #             # os.system(cmd)
        #             # cmd = f'kwgrib2 {os.path.join(self.data_root, fn)} -var -ftime'
        #             # os.system(cmd)
        #             cmd = f'./kwgrib2 {os.path.join(self.data_root, fn)} -lon {lon} {lat}'
        #             data = os.popen(cmd).read()
        #             # print(data)
        #             data = data.split(':')[2:]
        #             # print(data)
        #             data[-1] = data[-1][:-1]
        #
        #             val = float(data[-1].split('=')[-1])
        #             col_lst.append(val)
        #
        #     df[key] = col_lst
            df[key] = res.values()
        df.index.name = 'dt'
        df.columns = df.columns.map(LDAPS_SOLAR_DEFAULT)

        return df
        #
        # #
        # # dt_index, array = self.read_gfs(fct_start_dt, fct_end_dt)
        # # data_array = self._process_latlon(array, lat, lon)
        # # weather_data = pd.DataFrame(data_array.reshape(-1, 4), index=dt_index)
        # # weather_data.columns = self.col_mapping
        # # weather_data.index.name = "dt"
        # # return weather_data
