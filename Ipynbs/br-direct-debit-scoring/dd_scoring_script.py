# my modules
from pandas.io.json import json_normalize
# load algorithms


def main(json_data):
    appdata = json_normalize(json_data)
    score = round(float(1) * 1000)
    return score
