import pandas as pd

from Data.shuffle import shuffle_data


def getData():
    measles = pd.read_csv("Data/all-measles-rates.csv")
    shuffle_data(measles.copy())
    shuffled_data = pd.read_csv("Data/shuffled_measles.csv")

    return shuffled_data

