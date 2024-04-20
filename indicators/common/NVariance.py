import numpy as np
from NAverage import NAverage

def NVariance(time_series: np.ndarray, n: int) -> float:
    if n <= 0:
        raise ValueError("n must be greater than 0")
    if len(time_series) == 0:
        raise ValueError("Time series is empty")
    if n > len(time_series):
        raise ValueError("n is greater than the length of the time series")

    # Calculate the average of the last n elements
    n_average = NAverage(time_series, n)

    # Calculate the variance using the average
    variance = np.sum((time_series[-n:] - n_average) ** 2) / n
    return variance
