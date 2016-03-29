from pandas import read_csv
from os import path
import sys
from os import listdir, chdir
from collections import OrderedDict
from pandas import DataFrame
import matplotlib.pyplot as plt
from random import randint, sample, seed

# data_directory = 'task_events'
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

# doc du lieu tu task_event, chon mot Machine ID
# roi tinh tong tat ca cac CPU va RAM request cho tat cac cac job tai tung thoi diem
machine_id = 1268205
tasks_dict = {}
samples_dicts = OrderedDict([])
sample_moments_iterator = iter(sample_moments)
current_sample_moment = next(sample_moments_iterator)

task_events_csv_colnames = ['time', 'missing', 'job_id', 'task_idx', 'machine_id', 'event_type', 'user', 'sched_cls',
                            'priority', 'cpu_requested', 'mem_requested', 'disk', 'restriction']

totalreadfile =0

cpu_request = 0.0
mem_request = 0.0
disk_request = 0.0
number_of_task_requested = 0
for fn in sorted(listdir(data_directory)):

    fp = path.join(data_directory, fn)

    task_events_df = read_csv(fp, header=None, index_col=False, compression='gzip',
                              names=task_events_csv_colnames)
    print 'reading file ' + fp
    totalreadfile = totalreadfile +1
    # task_events_df = task_events_df[task_events_df['machine_id']==machine_id]
    for index, event in task_events_df.iterrows():

        while current_sample_moment is not None and event['time'] > current_sample_moment:
            # tmp_tasks_df = DataFrame(tasks_dict.values())
            # tmp_tasks_df = tmp_tasks_df[tmp_tasks_df['machine_id']==machine_id]
            # dong duoi nay co the toi uu hon
            samples_dicts[current_sample_moment] = ({'time' : current_sample_moment,
                                                     'cpu_requested' : cpu_request,
                                                     'mem_requested' : mem_request,
                                                     'disk_space_requested': disk_request,
                                                     'number_of_task_requested': number_of_task_requested})

            try:
                current_sample_moment = next(sample_moments_iterator)
            except StopIteration:
                current_sample_moment = None


        # if event['machine_id']== machine_id:
        if event['event_type'] in [0, 7, 8]:
            if tasks_dict.has_key((event['job_id'], event['task_idx'])):
                if tasks_dict[(event['job_id'], event['task_idx'])]['machine_id'] == machine_id:
                    if tasks_dict[(event['job_id'], event['task_idx'])]['cpu_requested']>0:
                        cpu_request = cpu_request - tasks_dict[(event['job_id'], event['task_idx'])]['cpu_requested']

                    if tasks_dict[(event['job_id'], event['task_idx'])]['mem_requested']>0:
                        mem_request = mem_request - tasks_dict[(event['job_id'], event['task_idx'])]['mem_requested']

                    if tasks_dict[(event['job_id'], event['task_idx'])]['disk']>0:
                        disk_request = disk_request - tasks_dict[(event['job_id'], event['task_idx'])]['disk']
                    number_of_task_requested = number_of_task_requested -1

            if event['machine_id'] == machine_id:
                if event['cpu_requested']>0:
                    cpu_request = cpu_request + event['cpu_requested']

                if event['mem_requested']>0:
                    mem_request = mem_request + event['mem_requested']

                if event['disk']>0:
                    disk_request = disk_request + event['disk']
                number_of_task_requested = number_of_task_requested+1

            tasks_dict[(event['job_id'], event['task_idx'])] = {'task_id' : (event['job_id'], event['task_idx']),
                                                                    'machine_id' : event['machine_id'],
                                                                    'cpu_requested' : event['cpu_requested'],
                                                                    'mem_requested' : event['mem_requested'],
                                                                    'disk': event['disk']}

        elif event['event_type'] in [2, 3, 4, 5, 6]:
            try:
                if tasks_dict.has_key((event['job_id'], event['task_idx'])):
                    if tasks_dict[(event['job_id'], event['task_idx'])]['machine_id'] == machine_id:
                        if tasks_dict[(event['job_id'], event['task_idx'])]['cpu_requested']>0:
                            cpu_request = cpu_request - tasks_dict[(event['job_id'], event['task_idx'])]['cpu_requested']

                        if tasks_dict[(event['job_id'], event['task_idx'])]['mem_requested']>0:
                            mem_request = mem_request - tasks_dict[(event['job_id'], event['task_idx'])]['mem_requested']

                        if tasks_dict[(event['job_id'], event['task_idx'])]['disk']>0:
                            disk_request = disk_request - tasks_dict[(event['job_id'], event['task_idx'])]['disk']
                        number_of_task_requested = number_of_task_requested -1
                del tasks_dict[(event['job_id'], event['task_idx'])]
            except:
                print "loi task ("+ str(event['job_id'])+", "+\
                      str(event['task_idx'])+") ko co trong cac task da submit vao machine nay" +"\n"
                pass

    if (totalreadfile == 50):
        samples_df = DataFrame(samples_dicts.values())
        print samples_df.info()
        try:
            samples_df.to_csv(path.join(results_directory,'machine_request_sampling_machineid_'+str(machine_id)+'_interval_'+str(interval)
                                        +'.csv'),index=False)
        except:
            print 'khong ghi duoc file csv'
        totalreadfile = 0

    if current_sample_moment is None:
        break

samples_df = DataFrame(samples_dicts.values())
print samples_df.info()
try:
    samples_df.to_csv(path.join(results_directory,'machine_request_sampling_machineid_'+str(machine_id)+'_interval_'+str(interval)
                                    +'.csv'),index=False)
except:
    print 'khong ghi duoc file csv'

print 'done'
