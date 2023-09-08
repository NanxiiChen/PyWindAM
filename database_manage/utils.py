import numpy as np

def wind_level(speed):
    speed = round(speed, 1)
    wind_level_map = np.array([
        [0.0, 0.2],
        [0.3, 1.5],
        [1.6, 3.3],
        [3.4, 5.4],
        [5.5, 7.9],
        [8.0, 10.7],
        [10.8, 13.8],
        [13.9, 17.1],
        [17.2, 20.7],
        [20.8, 24.4],
        [24.5, 28.4],
        [28.5, 32.6],
        [32.7, 36.9],
        [37.0, 41.4],
        [41.5, 46.1],
        [46.2, 50.9],
        [51.0, 56.0],
        [56.1, 1e10],
    ])
    tag = np.where((speed >= wind_level_map[:, 0]) & (speed <= wind_level_map[:, 1]))[0]
    return float(tag[0])