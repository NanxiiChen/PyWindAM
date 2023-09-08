import os
import time
import logging

from configs import *
import database_manage as dm


logging.basicConfig(filename='logs.log',
                    level=logging.INFO,
                    format='%(asctime)s %(message)s')


def process_data_of_today(some_day):
    global ftp_downloader
    global DBURL, TOKEN, ORG, BUCKET, BATCH_SIZE
    ftp_downloader.download_one_day(some_day,
                                    local_root=LOCAL_ROOT,
                                    remote_root=REMOTE_ROOT)
    dbs_file_path = LOCAL_ROOT + "DBS/" + some_day + ".csv"
    try:

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

    except Exception as e:
        logging.error("Error in parsing file: " + some_day + ".csv")
        logging.error(e)

    try:
        ptp_file_path = LOCAL_ROOT + "PTP/" + some_day + ".csv"
        ptp_parser = dm.PTPFileParser(ptp_file_path, max_length=MAX_LENGTH,
                                      time_interval=INTERVAL*3, bridge_align=BRIDGE_ALIGN)

        mean_datas = ptp_parser.extract_mean_data(some_day)
        for length, df in mean_datas.items():
            df.fillna(value=-1.0, inplace=True)
            dm.write_file(url=DBURL, token=TOKEN, org=ORG, bucket=BUCKET,
                          dataframe=df, measurement="PTP", tag="length",
                          height_or_length=length,
                          logger=logging, batch_size=BATCH_SIZE)
    except Exception as e:
        logging.error("Error in parsing file: " + some_day + ".csv")
        logging.error(e)


if not os.path.exists(LOCAL_ROOT):
    os.makedirs(LOCAL_ROOT)
if not os.path.exists(LOCAL_ROOT + "DBS/"):
    os.makedirs(LOCAL_ROOT + "DBS/")
if not os.path.exists(LOCAL_ROOT + "PTP/"):
    os.makedirs(LOCAL_ROOT + "PTP/")


while True:

    now = time.strftime("%H:%M:%S", time.localtime())

    # setup: update all the missed files
    ftp_downloader = dm.FTPDownloader(FTP_ADDRESS)
    ftp_downloader.login(FTP_USERNAME, FTP_PASSWORD)

    logging.info("System awake.")
    print("System awake.")

    diff = ftp_downloader.detect_diff(remote_root=REMOTE_ROOT,
                                      local_root=LOCAL_ROOT)
    diff = list(filter(lambda x: int(x) >= int(DATE_START), diff))
    print(diff)

    if len(diff) != 0:
        for some_day in diff:
            process_data_of_today(some_day)

    # update: update the file of today, cover the earlier time points of today
    today = time.strftime("%Y%m%d", time.localtime())
    tag = True    # Maybe the file today does not exist at remote server
    while tag:
        try:
            process_data_of_today(today)
            tag = False
        except Exception as e:
            logging.error("Error in downloading file: " + today + ".csv")
            logging.error(e)
            time.sleep(3600)

    ftp_downloader.quit()
    time.sleep(60*60)
