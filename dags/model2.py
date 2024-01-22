from model1 import calculate_statistics
import pandas as pd
import numpy as np
import os
import sys
import json


def calculate_z_score(data_credit, mean_age, std_age):

    z_score = (data_credit["CustAge"] - mean_age) / std_age
    return str(z_score)


if __name__ == "__main__":

    datapath = os.getcwd() + os.sep + "data"
    filepath = datapath + os.sep + "credit-data.csv"

    data_credit = pd.read_csv(filepath)  # correct
    mean_age, std_age = calculate_statistics(data_credit)

    z_score = calculate_z_score(data_credit, mean_age, std_age)
    print(z_score)
