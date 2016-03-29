from pandas import read_csv
from os import path
import sys
from os import listdir, chdir
from collections import OrderedDict
from pandas import DataFrame
import matplotlib as mpl
mpl.use('Agg')   # 2 lenh tren phai thuc hien truoc khi import pylot
import matplotlib.pyplot as plt

samples_df = read_csv('results/machine_request_sampling_machineid_1268205_interval_1.csv',index_col=False)


fig = plt.figure()
ax = fig.add_subplot(111)
ax.plot(samples_df['time']/1000000, samples_df['cpu_requested'], label='cpu requested')
plt.xlim(min(samples_df['time'])/1000000, max(samples_df['time'])/1000000)
plt.legend()
fig.savefig('results/cpu_requested_machineid_1268205_interval_1.svg')
plt.close(fig) # nen close fig lai de ko bi ton RAM

fig = plt.figure(2)
ax = fig.add_subplot(111)
ax.plot(samples_df['time']/1000000, samples_df['mem_requested'], label='mem requested')
plt.xlim(min(samples_df['time'])/1000000, max(samples_df['time'])/1000000)
plt.legend()
fig.savefig('results/mem_requested_machineid_1268205_interval_1.svg')
plt.close(fig) # nen close fig lai de ko bi ton RAM

fig = plt.figure(3)
ax = fig.add_subplot(111)
ax.plot(samples_df['time']/1000000, samples_df['disk_space_requested'], label='disk space requested')
plt.xlim(min(samples_df['time'])/1000000, max(samples_df['time'])/1000000)
plt.legend()
fig.savefig('results/disk_space_requested_machineid_1268205_interval_1.svg')
plt.close(fig) # nen close fig lai de ko bi ton RAM

fig = plt.figure(3)
ax = fig.add_subplot(111)
ax.plot(samples_df['time']/1000000, samples_df['number_of_task_requested'], label='number of task requested')
plt.xlim(min(samples_df['time'])/1000000, max(samples_df['time'])/1000000)
plt.legend()
fig.savefig('results/number_of_task_requested_machineid_1268205_interval_1.svg')
plt.close(fig) # nen close fig lai de ko bi ton RAM

#
#
# # ax.plot(samples_df['time']/1000000, samples_df['number_of_task_requested'], label=' number_of_task_requested')
# plt.xlim(min(samples_df['time'])/1000000, max(samples_df['time'])/1000000)
# plt.title('machine ID = 1268205')
# plt.legend()
# fig.savefig('results/cpu_percentage_usage_machineid_1268205_interval_1.svg')
# plt.close(fig) # nen close fig lai de ko bi ton RAM
# plt.show()