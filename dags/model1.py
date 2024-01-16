import pandas as pd
import numpy as np
import os
import sys


def calculate_statistics(data_credit):

    mean_age = np.mean(data_credit["CustAge"])
    std_age = np.std(data_credit["CustAge"])
    return mean_age, std_age


if __name__ == "__main__":

    datapath = os.getcwd() + os.sep + "data"
    filepath = datapath + os.sep + "credit-data.csv"

    data_credit = pd.read_csv(filepath)
    mean_age, std_age = calculate_statistics(data_credit)
    print(mean_age, std_age)
