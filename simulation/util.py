#
# Copyright 2013 The Regents of The University California
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import random
import Queue
import collections
import numpy


#meaningless for central schedulers, won't matter if algorithm doesn't use it
NUM_SCHEDULERS = 5
MEDIAN_TASK_DURATION = 100
TASKS_PER_JOB = 100


class TaskDistributions:
    EXP_TASKS, EXP_JOBS, CONSTANT, LIST = range(4)

class Job(object):
    job_count = 0
    def __init__(self, num_tasks, start_time, task_distribution, median_task_duration, scheduler=0, task_distribution_times=[]):
        self.id = Job.job_count
        Job.job_count += 1
        self.num_tasks = num_tasks
        self.completed_tasks_count = 0
        self.start_time = start_time
        self.end_time = start_time
        self.unscheduled_tasks = []
        # TODO: This won't be correctly populated when tasks are stolen.
        self.time_all_tasks_scheduled = 0
        self.last_probe_reply_time= 0
        # Change this line to change to distribution of task durations.
        if task_distribution == TaskDistributions.EXP_TASKS:
            self.exponentially_distributed_tasks(median_task_duration)
        elif task_distribution == TaskDistributions.EXP_JOBS:
            self.exponentially_distributed_jobs(median_task_duration)
        elif task_distribution == TaskDistributions.CONSTANT:
            self.constant_distributed_tasks(median_task_duration)
        elif task_distribution == TaskDistributions.LIST:
            self.list_distributed_tasks(task_distribution_times)
        self.longest_task = max(self.unscheduled_tasks)
        self.job_id_hash = random.randint(1, int(1.0e18))
        self.scheduler = scheduler
        self.probed_workers = dict()

    def add_probe_response(self, worker, current_time):
        assert(self.probed_workers[worker.id] > 0)
        self.probed_workers[worker.id] -= 1
        if(self.probed_workers[worker.id] == 0):
            del self.probed_workers[worker.id]
        assert current_time >= self.last_probe_reply_time
        self.last_probe_reply_time = current_time
        if len(self.unscheduled_tasks) > 0:
            assert current_time >= self.time_all_tasks_scheduled
            self.time_all_tasks_scheduled = current_time

    def task_completed(self, completion_time):
        """ Returns true if the job has completed, and false otherwise. """
        self.completed_tasks_count += 1
        self.end_time = max(completion_time, self.end_time)
        assert self.completed_tasks_count <= self.num_tasks
        return self.num_tasks == self.completed_tasks_count

    def list_distributed_tasks(self, task_duration_times):
        assert(self.num_tasks == len(task_duration_times))
        self.unscheduled_tasks = list(task_duration_times)

    def exponentially_distributed_tasks(self, median_task_duration):
        while len(self.unscheduled_tasks) < self.num_tasks:
            # Exponentially distributed task durations.
            self.unscheduled_tasks.append(random.expovariate(1.0 / median_task_duration))

    def exponentially_distributed_jobs(self, median_task_duration):
        # Choose one exponentially-distributed task duration for all tasks in the job.
        task_duration = random.expovariate(1.0 / median_task_duration)
        while len(self.unscheduled_tasks) < self.num_tasks:
            self.unscheduled_tasks.append(task_duration)

    def constant_distributed_tasks(self, median_task_duration):
        while len(self.unscheduled_tasks) < self.num_tasks:
            self.unscheduled_tasks.append(median_task_duration)

class JobGenerator(object):
    """ Abstract class representing Job generator. """
    def __init__(self):
        raise NotImplementedError("JobGenerator is an abstract class and cannot be "
                                  "instantiated directly")

    def get_next_job(self, current_time):
        """ Returns next job that arrives. """
        raise NotImplementedError("The get_next_job() method must be implemented by "
                                  "each class subclassing JobGenerator")


class JobsFromDistribution(JobGenerator):
    def __init__(self, interarrival_delay, task_distribution):
        self.interarrival_delay = interarrival_delay
        self.task_distribution = task_distribution

    def get_next_job(self, current_time):
        random.seed(current_time)
        scheduler = random.randint(0, NUM_SCHEDULERS-1)
        return Job(TASKS_PER_JOB, current_time, self.task_distribution, MEDIAN_TASK_DURATION, scheduler)

    def get_next_job_delay(self, current_time):
        random.seed(current_time)
        arrival_delay = random.expovariate(1.0 / self.interarrival_delay)
        return arrival_delay
        
        
class JobsFromTrace(JobGenerator):
    def __init__(self, filename):
        self.job_deque = collections.deque()
        with open(filename) as f:
            for line in f:
                self.job_deque.append(line)

    def get_next_job(self, current_time):
        line = self.job_deque.popleft()
        tokens = line.split()
        job_submission_time = float(tokens[0])
        assert(job_submission_time >= current_time)
        num_tasks = int(tokens[1])
        assert (len(tokens) == num_tasks + 3)
        avg_task_duration = float(tokens[2])
        task_duration_times = []
        for i in range(num_tasks):
            task_duration_times.append(float(tokens[3+i]))
        scheduler = random.randint(0, NUM_SCHEDULERS-1)
        #if(avg_task_duration > 1000.0):
        #print job_submission_time, avg_task_duration, num_tasks, numpy.median(task_duration_times), scheduler
        #print task_duration_times
        return Job(num_tasks, job_submission_time, TaskDistributions.LIST, 0, scheduler, task_duration_times)
           
    def get_next_job_delay(self, current_time):
        if(len(self.job_deque) == 0):
            return None
        line = self.job_deque.popleft()
        self.job_deque.appendleft(line) #can't just see the left element :(
        tokens = line.split()
        job_submission_time = float(tokens[0])
        return job_submission_time - current_time


        

