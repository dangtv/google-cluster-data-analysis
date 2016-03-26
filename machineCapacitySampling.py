#lay thong tin CPU, RAM available cua machine
from pandas import read_csv
from os import path
import sys
from os import listdir, chdir
from collections import OrderedDict
from pandas import DataFrame
from random import randint, sample, seed

# data_directory = 'machine_events'
# results_directory = 'results'
if(len(sys.argv) <4):
    print 'chua thiet lap tham so: data directory, results directory, sampling interval (second)'
    exit()
data_directory = sys.argv[1]
results_directory = sys.argv[2]
interval = sys.argv[3]

maxtime_stamp = 2506199602822

seed(83)
sample_moments = range(0,maxtime_stamp,1000000* (int)(interval)) # lay mau theo giay
# sample_moments = sorted(sample(xrange(maxtime_stamp), 200))

machine_id = 1268205
machines_dict = {}
samples_dicts = OrderedDict([])
sample_moments_iterator = iter(sample_moments)
current_sample_moment = next(sample_moments_iterator)

machine_events_csv_colnames=['time', 'machine_id', 'event_type', 'platform_id', 'cpu', 'mem']

# %%time
for fn in sorted(listdir(data_directory)):
    fp = path.join(data_directory, fn)
    machine_events_df = read_csv(fp, header=None, index_col=False, compression='gzip', names=machine_events_csv_colnames)
    print 'reading file '+fp
    for index, event in machine_events_df.iterrows():

        while current_sample_moment is not None and event['time'] > current_sample_moment:
            tmp_machines_df = DataFrame(machines_dict.values())
            if not tmp_machines_df.empty:
                samples_dicts[current_sample_moment] = ({'time' : current_sample_moment,
                                                         'cpu_available' : (tmp_machines_df['cpu']).sum(),
                                                         'mem_available' : (tmp_machines_df['mem']).sum()})
            else:
                samples_dicts[current_sample_moment] = ({'cpu_available' : 0,
                                                         'mem_available' : 0})
            try:
                current_sample_moment = next(sample_moments_iterator)
            except StopIteration:
                current_sample_moment = None
        if event['machine_id'] == machine_id:
            if event['event_type'] in [0, 2]:
                machines_dict[event['machine_id']] = {'machine_id' : event['machine_id'],
                                                      'cpu' : event['cpu'], 'mem' : event['mem']}
            elif event['event_type'] in [1]:
                del machines_dict[event['machine_id']]

    if current_sample_moment is None:
        break

samples_df = DataFrame(samples_dicts.values())
print samples_df.info()
try:
    samples_df.to_csv(path.join(results_directory,'machine_capacity_sampling_machineid_'+str(machine_id)+'_interval_'+str(interval)
                                    +'.csv'),index=False)
except:
    print 'khong ghi duoc file csv'