from pandas import read_csv
from os import path
import sys
from os import listdir, chdir
from collections import OrderedDict
from pandas import DataFrame
import matplotlib.pyplot as plt

samples_df = read_csv('results/machine_usage_sampling_machineid_1268205_interval_1.csv',index_col=False)

fig = plt.figure()
ax = fig.add_subplot(111)
# ax.plot(samples_df['time'], samples_df['cpu_requested'], label='cpu requested')
# ax.plot(samples_df['time'], samples_df['cpu_available'], label='cpu available')
ax.plot(samples_df['time']/1000000, samples_df['cpu_usage'], label='cpu usage')
plt.xlim(min(samples_df['time'])/1000000, max(samples_df['time'])/1000000)
plt.legend()
plt.show()
