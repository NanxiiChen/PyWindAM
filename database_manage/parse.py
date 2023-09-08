"""
parse.py
parse data from csv files
"""

import re
import numpy as np
import pandas as pd

from database_manage.utils import wind_level


class DBSFileParser:

    def __init__(self, csv_path, max_height=250, bridge_align=40, time_interval=600):
        df = pd.read_csv(csv_path, skiprows=1).dropna(how="any", inplace=False)
        # reindex datafrome
        df.index = range(df.shape[0])
        heights = df.columns[7:]
        heights = [int(re.findall('([0-9]+)', height)[0])
                   for height in heights[::4]]
        heights = list(filter(lambda x: x <= max_height, heights))
        data_each_height = {height: df[df.columns[7:][4*i: 4*i+4]]
                            for i, height in enumerate(heights)}

        for height in heights:
            val = data_each_height[height].copy()

            val.columns = ["u", "beta", "db", "zwind"]
            val.loc[:, "time"] = df["Date_time"].values
            for i in range(4):
                val.index = range(val.shape[0])
                val.drop(
                    np.where(np.abs(val.iloc[:, i] - 999) < 1e-3)[0], inplace=True, axis=0)

            thetas = np.deg2rad(val["beta"].values - bridge_align)  # 桥轴向为0度
            val["ux"] = val["u"] * np.cos(thetas)
            val["uy"] = val["u"] * np.sin(thetas)
            val["time"] = val["time"].apply(lambda x: x.split(".")[0])
            val["time"] = pd.to_datetime(val["time"])
            data_each_height[height] = val

        self.data_each_height = data_each_height
        self.time_interval = time_interval

    def compute_interval_mean_data(self, u, beta):
        U_extreme = np.max(u)
        beta_rad = np.deg2rad(beta)
        ux = u * np.cos(beta_rad)
        uy = u * np.sin(beta_rad)
        ux_mean = np.mean(ux)
        uy_mean = np.mean(uy)
        U_mean = np.sqrt(ux_mean**2 + uy_mean**2)
        beta_mean = np.arctan2(uy_mean, ux_mean) \
            if np.arctan2(uy_mean, ux_mean) >= 0 \
            else np.arctan2(uy_mean, ux_mean) + 2*np.pi
        u1 = ux * np.cos(beta_mean) + uy * np.sin(beta_mean) - U_mean
        u2 = -ux * np.sin(beta_mean) + uy * np.cos(beta_mean)
        beta_mean = np.rad2deg(beta_mean)
        Iu = np.std(u1) / U_mean
        Gu = np.max(u1) / U_mean + 1
        Iv = np.std(u2) / U_mean
        Gv = np.max(u2) / U_mean

        extreme_level = wind_level(U_extreme)
        mean_level = wind_level(U_mean)

        return [U_extreme, U_mean, beta_mean, Iu, Gu, Iv, Gv, extreme_level, mean_level]

    def make_time_interval(self, date):
        interval = self.time_interval
        starts = pd.date_range(date, periods=24*3600 //
                               interval, freq=f"{interval}s")
        ends = starts + pd.Timedelta(seconds=interval)
        return starts, ends
        # return pd.date_range(date, periods=24*3600//interval-1, freq=f"{interval}s")

    def extract_mean_data(self, today):
        mean_datas = {}
        heights = list(self.data_each_height.keys())
        starts, ends = self.make_time_interval(date=today)
        for height in heights:
            mean_data = []
            val = self.data_each_height[height].copy()
            for start, end in zip(starts, ends):
                val_interval = val[(val["time"] >= start)
                                   & (val["time"] < end)]
                if len(val_interval) < 10:
                    mean_data.append([start, end]
                                     + [np.nan] * 11)
                else:
                    mean_data.append([
                        start, end,
                        np.mean(np.abs(val_interval["ux"].values)),
                        np.mean(np.abs(val_interval["uy"].values)),
                    ] + self.compute_interval_mean_data(val_interval["u"].values,
                                                        val_interval["beta"].values))
            mean_datas[height] = pd.DataFrame(mean_data,
                                              columns=["start_time", "end_time",
                                                       "ux_mean", "uy_mean",
                                                       "U_extreme", "U_mean", "beta_mean",
                                                       "Iu", "Gu", "Iv", "Gv",
                                                       "extreme_level", "mean_level"])

        return mean_datas


class PTPFileParser:
    def __init__(self, csv_path, max_length=2000, bridge_align=40, time_interval=600):
        df = pd.read_csv(csv_path, skiprows=1).dropna(how="any", inplace=False)
        # reindex datafrome
        df.index = range(df.shape[0])
        lengths = list(range(60, max_length, 30))
        data_each_length = {height: df[df.columns[7:][4*i: 4*i+4]]
                            for i, height in enumerate(lengths)}

        for length in lengths:
            val = data_each_length[length].copy()

            val.columns = ["u", "beta", "db", "zwind"]
            val.loc[:, "time"] = df["Date_time"].values
            for i in range(4):
                val.index = range(val.shape[0])
                val.drop(
                    np.where(np.abs(val.iloc[:, i] - 999) < 1e-3)[0], inplace=True, axis=0)

            thetas = np.deg2rad(val["beta"].values - bridge_align)
            val["ux"] = val["u"] * np.cos(thetas)
            val["uy"] = val["u"] * np.sin(thetas)
            val["time"] = val["time"].apply(lambda x: x.split(".")[0])
            val["time"] = pd.to_datetime(val["time"])
            data_each_length[length] = val

        self.data_each_length = data_each_length
        self.time_interval = time_interval

    def compute_interval_mean_data(self, u, beta, zwind):
        U_extreme = np.max(u)
        beta_rad = np.deg2rad(beta)
        ux = u * np.cos(beta_rad)
        uy = u * np.sin(beta_rad)
        ux_mean = np.mean(ux)
        uy_mean = np.mean(uy)
        uz_mean = np.mean(zwind)
        U_mean = np.sqrt(ux_mean**2 + uy_mean**2)
        beta_mean = np.arctan2(uy_mean, ux_mean) \
            if np.arctan2(uy_mean, ux_mean) >= 0 \
            else np.arctan2(uy_mean, ux_mean) + 2*np.pi
        alpha_mean = np.arctan2(uz_mean, ux_mean)
        u1 = ux * np.cos(beta_mean) + uy * np.sin(beta_mean) - U_mean
        u2 = -ux * np.sin(beta_mean) + uy * np.cos(beta_mean)
        beta_mean = np.rad2deg(beta_mean)
        alpha_mean = np.rad2deg(alpha_mean)
        Iu = np.std(u1) / U_mean
        Gu = np.max(u1) / U_mean + 1
        Iv = np.std(u2) / U_mean
        Gv = np.max(u2) / U_mean

        extreme_level = wind_level(U_extreme)
        mean_level = wind_level(U_mean)

        return [U_extreme, U_mean, beta_mean, alpha_mean, Iu, Gu, Iv, Gv, extreme_level, mean_level]

    def make_time_interval(self, date):
        interval = self.time_interval
        starts = pd.date_range(date, periods=24*3600 //
                               interval, freq=f"{interval}s")
        ends = starts + pd.Timedelta(seconds=interval)
        return starts, ends
        # return pd.date_range(date, periods=24*3600//interval-1, freq=f"{interval}s")

    def extract_mean_data(self, today):
        mean_datas = {}
        lengths = list(self.data_each_length.keys())
        starts, ends = self.make_time_interval(date=today)
        for length in lengths:
            mean_data = []
            val = self.data_each_length[length].copy()
            for start, end in zip(starts, ends):
                val_interval = val[(val["time"] >= start)
                                   & (val["time"] < end)]
                if len(val_interval) < 5:
                    mean_data.append([start, end]
                                     + [np.nan] * 12)
                else:
                    mean_data.append([
                        start, end,
                        np.mean(np.abs(val_interval["ux"].values)),
                        np.mean(np.abs(val_interval["uy"].values)),
                    ] + self.compute_interval_mean_data(val_interval["u"].values,
                                                        val_interval["beta"].values,
                                                        val_interval["zwind"].values))
            mean_datas[length] = pd.DataFrame(mean_data,
                                              columns=["start_time", "end_time",
                                                       "ux_mean", "uy_mean",
                                                       "U_extreme", "U_mean", "beta_mean", "alpha_mean",
                                                       "Iu", "Gu", "Iv", "Gv",
                                                       "extreme_level", "mean_level"])

        return mean_datas
