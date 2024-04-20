from numpy import ndarray
import numpy as np

def log_delta(time_series: ndarray) -> ndarray:
    if len(time_series) < 2:
        raise ValueError("Time series must contain at least two data points.")

    # Convert the input list to a numpy array if it isn't already one
    if not isinstance(time_series, ndarray):
        time_series = np.array(time_series, dtype=float)

    # Calculate the differences in log-represent rate
    log_deltas = np.log(time_series[1:] / time_series[:-1])

    return log_deltas