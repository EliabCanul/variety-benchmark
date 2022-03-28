import pandas as pd
import numpy as np
import warnings

# Refer to read.me info
def mape(y_true, y_pred, remove_zero_targets = True):
    """Computes mean absolute percent error.

    Args:
        y_true (np.array): Target
        y_pred (np.array): Predictions
        remove_zero_targets (bool, optional): If True, does not count 0s as part of the benchmark. If false, maps
        0s to 1 to avoid divide by inifinity error Defaults to True.
        name (Str): Name of metric that will be displayed on 

    Returns:
        Float: Mean absolute percent error
    """
    # remove_zeros
    if remove_zero_targets:
        if len(y_true) > 1:
            nonZeroIndices = np.array([ind for ind, val in enumerate(y_true == 0) if val == 0])
        else:
            return (np.abs(y_true - y_pred)/y_true)[0]
    else:
        for i in range(len(y_true)):
            if y_true[i] == 0:
                y_true[i] += 1
                y_pred[i] += 1

    return np.mean(np.abs((y_true[nonZeroIndices] - y_pred[nonZeroIndices]) / y_true[nonZeroIndices])) * 100


