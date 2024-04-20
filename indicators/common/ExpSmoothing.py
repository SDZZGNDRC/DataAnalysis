import numpy as np

def exp_smoothing(time_series: np.ndarray, alpha: float) -> np.ndarray:
    if not 0 < alpha < 1:
        raise ValueError("Alpha must be between 0 and 1")
    if len(time_series) == 0:
        raise ValueError("Time series is empty")

    # Initialize the array for smoothed values with the same length as time_series
    # and set the first smoothed value to the first value of the time series.
    smoothed_values = np.zeros_like(time_series)
    smoothed_values[0] = time_series[0]

    # Calculate the smoothed values for the rest of the time series
    for t in range(1, len(time_series)):
        smoothed_values[t] = alpha * time_series[t] + (1 - alpha) * smoothed_values[t - 1]

    return smoothed_values
