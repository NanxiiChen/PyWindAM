import os
import time
import logging
import datetime

import argparse

from configs import *
import database_manage as dm

arg_parser = argparse.ArgumentParser()
arg_parser.add_argument("--end_day", type=str,
                        default=time.strftime("%Y%m%d", time.localtime()))
args = arg_parser.parse_args()
end_day = args.end_day

print(f"Start uploading data from {DATE_START} to {end_day}.")


logging.basicConfig(filename='logs.log',
                    level=logging.INFO,
                    format='%(asctime)s %(message)s')


def upload_files(some_day):
    global ftp_downloader
    global DBURL, TOKEN, ORG, BUCKET, BATCH_SIZE

    dbs_file_path = LOCAL_ROOT + "DBS/" + some_day + ".csv"

    if os.path.exists(dbs_file_path):
        dbs_parser = dm.DBSFileParser(dbs_file_path, max_height=MAX_HEIGHT,
                                      time_interval=INTERVAL, bridge_align=BRIDGE_ALIGN)
        mean_datas = dbs_parser.extract_mean_data(some_day)

        # measure: DBS or PTP
        # tag: each height
        for height, df in mean_datas.items():
            df.fillna(value=-1.0, inplace=True)
            dm.write_file(url=DBURL, token=TOKEN, org=ORG, bucket=BUCKET,
                          dataframe=df, measurement="DBS", tag="height",
                          height_or_length=height,
                          logger=logging, batch_size=BATCH_SIZE)
    else:
        logging.info(f"File {dbs_file_path} not exists.")
        print(f"File {dbs_file_path} not exists.")

    ptp_file_path = LOCAL_ROOT + "PTP/" + some_day + ".csv"

    if os.path.exists(ptp_file_path):

        ptp_parser = dm.PTPFileParser(ptp_file_path, max_length=MAX_LENGTH,
                                      time_interval=INTERVAL*3, bridge_align=BRIDGE_ALIGN)

        mean_datas = ptp_parser.extract_mean_data(some_day)
        for length, df in mean_datas.items():
            df.fillna(value=-1.0, inplace=True)
            dm.write_file(url=DBURL, token=TOKEN, org=ORG, bucket=BUCKET,
                          dataframe=df, measurement="PTP", tag="length",
                          height_or_length=length,
                          logger=logging, batch_size=BATCH_SIZE)
    else:
        logging.info(f"File {ptp_file_path} not exists.")
        print(f"File {ptp_file_path} not exists.")


this_day = DATE_START


while this_day <= end_day:
    upload_files(this_day)
    this_day = (datetime.datetime.strptime(this_day, "%Y%m%d") +
                datetime.timedelta(days=1)).strftime("%Y%m%d")
