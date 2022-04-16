import pandas as pd
from scipy import stats
import matplotlib.pyplot as plt
def run_ttest(df, cols):
    df = df[cols]
    length = len(df)
    print('full length ',length)
    sample = df.iloc[::15, :]
    print('main df ttest', stats.ttest_ind(df, sample))

