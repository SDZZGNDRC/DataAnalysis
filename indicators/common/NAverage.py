import numpy as np

def NAverage(time_series: np.ndarray, n: int) -> float:
    if n <= 0:
        raise ValueError("n must be greater than 0")
    if len(time_series) == 0:
        raise ValueError("Time series is empty")
    if n > len(time_series):
        raise ValueError("n is greater than the length of the time series")

    # Take the last n elements and calculate the average
    return np.mean(time_series[-n:])
