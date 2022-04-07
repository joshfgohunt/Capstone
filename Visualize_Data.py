import pandas as pd
from scipy import stats
import matplotlib.pyplot as plt
def run_zscore(df, cols):
    df = df[cols]
    length = len(df)
    samp = int(length/15)
    print('full length ',length)
    print('sample size ',samp)
    print('df Z-Scores')
    print(stats.zscore(df).sample(samp).mean())

