import io
import timeit
from ftplib import FTP
from multiprocessing import Pool

import boto3
from retrying import retry

from loader.ldaps_variable import LDAPS_GRIB


class syncfs(object):
    S3_BUCKET_NAME = 's3.ldaps'
    FTP_HOST = "ncms.kma.go.kr"
    FTP_PORT = 10021
    FTP_USERNAME = "Anonymous"
    FTP_PASSWORD = "Anonymous@"
    vars = [val['shortName'] for val in LDAPS_GRIB.values()]

    @retry(stop_max_attempt_number=50, wait_random_max=100)
    def fn_lst(self):
        try:
            ftp = FTP()
            res = ftp.connect(self.FTP_HOST, self.FTP_PORT)
            res = ftp.login()
            res = ftp.nlst('LDPS/UNIS')
            res = [path for path in res if path.split('_')[2] in self.vars]
            # res = [path for path in res if path.split('.')[-2] == '2021112118']
        finally:
            ftp.close()
        return res

    @retry(stop_max_attempt_number=50, wait_random_max=500)
    def download_data(self, fn):
        s3 = boto3.client("s3")
        ftp = FTP()
        res = ftp.connect(self.FTP_HOST, self.FTP_PORT)
        res = ftp.login()
        ftp_file_size = ftp.size(fn)
        file_name = fn.split("/")[-1]
        try:
            s3_file = s3.head_object(Bucket=self.S3_BUCKET_NAME, Key=f"{fn.split('.')[-2]}/{file_name}")
            if s3_file["ContentLength"] == ftp.size(fn):
                # print(f"{file_name} Already Exists in S3 bucket")
                return
        except:
            pass

        buff = io.BytesIO()
        ftp.retrbinary('RETR %s' % fn, buff.write)
        if ftp_file_size == buff.getbuffer().nbytes:
            buff.seek(0)
            s3.upload_fileobj(buff, self.S3_BUCKET_NAME, f"{fn.split('.')[-2]}/{file_name}")
            print(f"{file_name} Transfer Complete")
        else:
            raise Exception(f'{file_name} Transfer Failed')
        ftp.close()
        return 0

    def parallel_download(self, fn_lst):
        print('parallel')
        p = Pool()
        r = p.map(self.download_data, fn_lst)
        return

    def __call__(self, *args, **kwargs):
        self.fn_lst()
        fn_lst = self.fn_lst()
        print(f"{len(fn_lst)} files")
        res = self.parallel_download(fn_lst)


if __name__ == '__main__':
    start = timeit.default_timer()
    job = syncfs()
    job()
    stop = timeit.default_timer()
    print('Time: ', stop - start)
