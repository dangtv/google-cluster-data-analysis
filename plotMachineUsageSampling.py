from pandas import read_csv
from os import path
import sys
from os import listdir, chdir
from collections import OrderedDict
from pandas import DataFrame
import matplotlib.pyplot as plt

samples_df = read_csv('results/machine_usage_sampling_machineid_1268205_interval_1.csv',index_col=False)

fig = plt.figure(1)
ax = fig.add_subplot(111)
# ax.plot(samples_df['time'], samples_df['cpu_requested'], label='cpu requested')
# ax.plot(samples_df['time'], samples_df['cpu_available'], label='cpu available')
ax.plot(samples_df['time']/1000000, samples_df['cpu_usage'], label='cpu usage')
plt.xlim(min(samples_df['time'])/1000000, max(samples_df['time'])/1000000)
plt.legend()
# plt.show()
#
fig = plt.figure(2)
ax = fig.add_subplot(111)
# ax.plot(samples_df['time'], samples_df['cpu_requested'], label='cpu requested')
# ax.plot(samples_df['time'], samples_df['cpu_available'], label='cpu available')
ax.plot(samples_df['time']/1000000, samples_df['mem_usage'], label='mem usage')
plt.xlim(min(samples_df['time'])/1000000, max(samples_df['time'])/1000000)
plt.legend()
# plt.show()
#
fig = plt.figure(3)
ax = fig.add_subplot(111)
# ax.plot(samples_df['time'], samples_df['cpu_requested'], label='cpu requested')
# ax.plot(samples_df['time'], samples_df['cpu_available'], label='cpu available')
ax.plot(samples_df['time']/1000000, samples_df['disk_io_time'], label='disk_io_time')
plt.xlim(min(samples_df['time'])/1000000, max(samples_df['time'])/1000000)
plt.legend()
# plt.show()
#
fig = plt.figure(4)
ax = fig.add_subplot(111)
# ax.plot(samples_df['time'], samples_df['cpu_requested'], label='cpu requested')
# ax.plot(samples_df['time'], samples_df['cpu_available'], label='cpu available')
ax.plot(samples_df['time']/1000000, samples_df['disk_space'], label='disk_space')
plt.xlim(min(samples_df['time'])/1000000, max(samples_df['time'])/1000000)
plt.legend()
# plt.show()
#
fig = plt.figure(5)
ax = fig.add_subplot(111)
# ax.plot(samples_df['time'], samples_df['cpu_requested'], label='cpu requested')
# ax.plot(samples_df['time'], samples_df['cpu_available'], label='cpu available')
ax.plot(samples_df['time']/1000000, samples_df['number_of_running_task'], label='number_of_running_task')
plt.xlim(min(samples_df['time'])/1000000, max(samples_df['time'])/1000000)
plt.legend()
plt.show()