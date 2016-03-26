from pandas import read_csv
from os import path
import sys
from os import listdir, chdir
from collections import OrderedDict
from pandas import DataFrame
import matplotlib.pyplot as plt
from random import randint, sample, seed

# data_directory = 'task_usage'
# results_directory = 'results'
if(len(sys.argv) <4):
    print 'chua thiet lap tham so: data directory, results directory, sampling interval (second)'
    exit()
data_directory = sys.argv[1]
results_directory = sys.argv[2]
interval = sys.argv[3]

maxtime_stamp = 2506199602822
# maxtime_stamp = 5611086346
seed(83)
sample_moments = range(0,maxtime_stamp,1000000* (int)(interval)) # lay mau theo giay
# sample_moments = sorted(sample(xrange(maxtime_stamp), 200))

# doc du lieu tu task_event, chon mot Machine ID
# roi tinh tong tat ca cac CPU va RAM request cho tat cac cac job tai tung thoi diem
machine_id = 1268205
tasks_dict = {}
samples_dicts = OrderedDict([])
sample_moments_iterator = iter(sample_moments)
current_sample_moment = next(sample_moments_iterator)

# doc resource usage cho mot machine bat ky
task_usage_csv_colnames=['starttime', 'endtime', 'job_id', 'task_idx', 'machine_id', 'cpu_usage', 'mem_usage',
                         'assigned_mem', 'unmapped_cache_usage', 'page_cache_usage', 'max_mem_usage', 'disk_io_time',
                         'mean_local_disk_space', 'max_cpu_usage', 'max_disk_io_time', 'cpi', 'mai', 'sampling_portion',
                         'agg_type', 'sampled_cpu_usage']


while current_sample_moment is not None:
    samples_dicts[current_sample_moment] = ({'time' : current_sample_moment, 'cpu_usage' : 0.0,
                                             'mem_usage' : 0.0, 'disk_io_time': 0.0, 'disk_space': 0.0})
    try:
        current_sample_moment = next(sample_moments_iterator)
    except StopIteration:
        current_sample_moment = None
# %%time
totalreadfile = 0
for fn in sorted(listdir(data_directory)):

    fp = path.join(data_directory, fn)
    task_usage_df = read_csv(fp, header=None, index_col=False, compression='gzip', names=task_usage_csv_colnames)
    print 'reading file ' + fp
    totalreadfile = totalreadfile+1
    task_usage_df = task_usage_df[task_usage_df['machine_id']==machine_id]
    # print task_usage_df.info()
    laststart = max(task_usage_df['endtime'])
    # if laststart > max(sample_moments) and laststart > snapshot_moment:
    #     break
    firststart = min(task_usage_df['starttime'])
    if firststart > maxtime_stamp:
        break
    for moment in samples_dicts:
        if moment < firststart:
            continue
        if moment >laststart:
            break
        task_usage_moment_df = task_usage_df[(task_usage_df['starttime'] <= moment) &
                                             (moment < task_usage_df['endtime'])]
        # print task_usage_moment_df.info()
        samples_dicts[moment]['cpu_usage'] += sum(task_usage_moment_df['cpu_usage'])
        samples_dicts[moment]['mem_usage'] += sum(task_usage_moment_df['mem_usage'])
        samples_dicts[moment]['disk_io_time'] += sum(task_usage_moment_df['disk_io_time'])
        samples_dicts[moment]['disk_space'] += sum(task_usage_moment_df['mean_local_disk_space'])
        samples_dicts[moment]['number_of_running_task'] += len(task_usage_moment_df['cpu_usage'])
    if (totalreadfile == 50):
        samples_df = DataFrame(samples_dicts.values())
        print samples_df.info()
        try:
            samples_df.to_csv(path.join(results_directory,'machine_usage_sampling_machineid_'+str(machine_id)+'_interval_'+str(interval)
                                            +'.csv'),index=False)
        except:
            print 'khong ghi duoc file csv'
        totalreadfile = 0

samples_df = DataFrame(samples_dicts.values())
print samples_df.info()
try:
    samples_df.to_csv(path.join(results_directory,'machine_usage_sampling_machineid_'+str(machine_id)+'_interval_'+str(interval)
                                    +'.csv'),index=False)
except:
    print 'khong ghi duoc file csv'

print 'done'