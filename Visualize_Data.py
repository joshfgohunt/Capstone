import pandas as pd

import matplotlib.pyplot as plt

def view_data_by_year(df, xLim,yLim):
    plt.subplots_adjust(left=0,
                    bottom=1, 
                    right=4, 
                    top=3, 
                    wspace=0.2, 
                    hspace=0.35)
    plt.subplot(1,3,1)
    plt.plot(range(100))

    plt.xlim(0, xLim)
    plt.ylim(0, yLim)
    plt.title('A Histogram of Rental Prices 2011')
    plt.hist(df['2011'], bins = 40,  color="green")

    plt.subplot(1,3,2)
    plt.plot(range(100))

    plt.xlim(0, xLim)
    plt.ylim(0, yLim)
    plt.title('A Histogram of Rental Prices 2016')
    plt.hist(df['2016'], bins = 40,  color="orange")

    plt.subplot(1,3,3)
    plt.plot(range(100))

    plt.xlim(0, xLim)
    plt.ylim(0, yLim)
    plt.title('A Histogram of Rental Prices 2020')
    plt.hist(df['2020'], bins = 40,  color="blue")