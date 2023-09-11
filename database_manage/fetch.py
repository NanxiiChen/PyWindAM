"""
fetch.py
fetch data from remote ftp server
"""


import os
import time
import logging
from ftplib import FTP


class FTPDownloader(FTP):

    def __init__(self, address):
        super().__init__(address)

    def _remote_file_dir_generator(self, some_day, remote_root):
        return remote_root + some_day + "/" + "level1"

    def _download_single_file(self, remote_fname, local_path, mode='wb'):
        logging.info(f"Downloading {remote_fname} to {local_path}")
        print(f"Downloading {remote_fname} to {local_path}")

        with open(local_path, mode) as f:
            self.retrbinary('RETR ' + remote_fname, f.write)

    def detect_diff(self, remote_root, local_root):
        self.cwd(remote_root)
        remote_days = self.nlst()
        local_fnames = os.listdir(local_root + "DBS/") \
            + os.listdir(local_root + "PTP/")
        local_days = list(map(lambda x: x.split("_")[-1][0:8], local_fnames))
        diff = list(set(remote_days) - set(local_days))
        diff.sort()
        logging.info(f"DIR DIFF: {','.join(diff)}")
        print(f"DIR DIFF: {','.join(diff)}")
        return diff

    def download_one_day(self, some_day, local_root, remote_root):

        remote_file_dir_path = self._remote_file_dir_generator(some_day,
                                                               remote_root=remote_root)

        self.cwd(remote_file_dir_path)
        fnames = self.nlst()
        dbs_files = list(filter(lambda x: "DBS" in x, fnames))
        ptp_files = list(filter(lambda x: "PointTo" in x, fnames))

        dbs_files.sort(key=lambda x: x.split(".")[0][-6:])
        local_path = local_root + "DBS/" + some_day + ".csv"
        mode = "wb"
        for idx, dbs_file in enumerate(dbs_files):
            if idx == 0:
                self._download_single_file(dbs_file, local_path, mode=mode)
            else:
                temp_path = local_root + "DBS/" + "_temp.csv"
                self._download_single_file(dbs_file, temp_path, mode=mode)
                with open(temp_path, "r") as f:
                    temp_lines = f.readlines()
                with open(local_path, "a") as f:
                    f.write("\n")
                    f.writelines(temp_lines[2:])
            time.sleep(5)

        ptp_files.sort(key=lambda x: x.split(".")[0][-6:])
        local_path = local_root + "PTP/" + some_day + ".csv"
        mode = "wb"
        for idx, ptp_file in enumerate(ptp_files):
            if idx == 0:
                self._download_single_file(ptp_file, local_path, mode=mode)
            else:
                temp_path = local_root + "PTP/" + "_temp.csv"
                self._download_single_file(ptp_file, temp_path, mode=mode)
                with open(temp_path, "r") as f:
                    temp_lines = f.readlines()
                with open(local_path, "a") as f:
                    f.write("\n")
                    f.writelines(temp_lines[2:])
            time.sleep(5)

    def download_diff(self, diff, local_root, remote_root):
        dbs_local_paths = []
        ptp_local_paths = []
        for some_day in diff:
            today = time.strftime("%Y%m%d", time.localtime())
            # skip today, because the recorded data is not complete
            if some_day == today:
                continue
            remote_file_dir_path = self._remote_file_dir_generator(some_day,
                                                                   remote_root)

            self.cwd(remote_file_dir_path)
            fnames = self.nlst()
            dbs_files = list(filter(lambda x: "DBS" in x, fnames))
            ptp_files = list(filter(lambda x: "PointTo" in x, fnames))

            dbs_files.sort(key=lambda x: x.split(".")[0][-6:])
            local_path = local_root + "DBS/" + some_day + ".csv"
            mode = "wb"
            # maybe more than one file for just one day
            for idx, dbs_file in enumerate(dbs_files):
                if idx == 0:
                    self._download_single_file(dbs_file, local_path, mode=mode)
                else:
                    temp_path = local_root + "DBS/" + "_temp.csv"
                    self._download_single_file(dbs_file, temp_path, mode=mode)
                    with open(temp_path, "r") as f:
                        temp_lines = f.readlines()
                    with open(local_path, "a") as f:
                        f.write("\n")
                        f.writelines(temp_lines[2:])
                time.sleep(5)
            dbs_local_paths.append(local_path)

            ptp_files.sort(key=lambda x: x.split(".")[0][-6:])
            local_path = local_root + "PTP/" + some_day + ".csv"
            mode = "wb"
            for idx, ptp_file in enumerate(ptp_files):
                if idx == 0:
                    self._download_single_file(ptp_file, local_path, mode=mode)
                else:
                    temp_path = local_root + "PTP/" + "_temp.csv"
                    self._download_single_file(ptp_file, temp_path, mode=mode)
                    with open(temp_path, "r") as f:
                        temp_lines = f.readlines()
                    with open(local_path, "a") as f:
                        f.write("\n")
                        f.writelines(temp_lines[2:])
                time.sleep(5)
            ptp_local_paths.append(local_path)

        return dbs_local_paths, ptp_local_paths
