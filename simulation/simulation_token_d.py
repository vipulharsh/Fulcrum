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

""" Simulations a random assignment of tasks to workers. """

import math
import numpy
import random
import Queue
import util
from util import Job, TaskDistributions, JobsFromDistribution, JobsFromTrace
from util import NUM_SCHEDULERS, TASKS_PER_JOB, MEDIAN_TASK_DURATION

NETWORK_DELAY = 0.5
SLOTS_PER_WORKER = 4
TOTAL_WORKERS = 1000
WARM_UP_TIME = 500

def get_percentile(N, percent, key=lambda x:x):
    if not N:
        return 0
    k = (len(N) - 1) * percent
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return key(N[int(k)])
    d0 = key(N[int(f)]) * (c-k)
    d1 = key(N[int(c)]) * (k-f)
    return d0 + d1

def plot_cdf(values, filename):
    values.sort()
    f = open(filename, "w")
    for percent in range(100):
        fraction = percent / 100.
        f.write("%s\t%s\n" % (fraction, get_percentile(values, fraction)))
    f.close()

class Event(object):
    """ Abstract class representing events. """
    def __init__(self):
        raise NotImplementedError("Event is an abstract class and cannot be "
                                  "instantiated directly")

    def run(self, current_time):
        """ Returns any events that should be added to the queue. """
        raise NotImplementedError("The run() method must be implemented by "
                                  "each class subclassing Event")

class JobArrival(Event):
    """ Event to signify a job arriving at a scheduler. """
    def __init__(self, simulation, interarrival_delay, task_distribution):
        self.job_generator = JobsFromDistribution(interarrival_delay, task_distribution)
        self.simulation = simulation

    def run(self, current_time):
        job = self.job_generator.get_next_job(current_time)
        #print "Job %s arrived at %s" % (job.id, current_time)
        # Schedule job.
        new_events = self.simulation.send_tasks(job, current_time)

        # Add new Job Arrival event, for the next job to arrive after this one.
        arrival_delay = self.job_generator.get_next_job_delay(current_time)
        if(arrival_delay != None):
            new_events.append((current_time + arrival_delay, self))
        else:
            self.simulation.no_more_jobs = True
        #print "Retuning %s events" % len(new_events)
        return new_events


class JobArrivalFile(Event):
    """ Event to signify a job arriving at a scheduler. """
    def __init__(self, simulation, tracefile):
        self.job_generator = JobsFromTrace(tracefile)
        self.simulation = simulation
        print "NUM_SCHEDULERS: ", NUM_SCHEDULERS

    def run(self, current_time):
        job = self.job_generator.get_next_job(current_time)
        if(job.id % 5000 == 0):
            print "Job %s arrived at %s" % (job.id, current_time)
        # Schedule job.
        new_events = self.simulation.send_tasks(job, current_time)
        # Add new Job Arrival event, for the next job to arrive after this one.
        arrival_delay = self.job_generator.get_next_job_delay(current_time)
        if(arrival_delay != None):
            new_events.append((current_time + arrival_delay, self))
        else:
            self.simulation.no_more_jobs = True
        #print "Retuning %s events" % len(new_events)
        return new_events

class HeartbeatEvent(Event):
    def __init__(self, simulation):
        self.simulation = simulation
        self.heartbeat_period = 1

    def run(self, current_time):
        new_events = self.simulation.redistribute_tokens(current_time)
        new_events.append((current_time + self.heartbeat_period, self))
        return new_events

class TokenArrival(Event):
    """ Event to signify a token arriving at a scheduler. """
    def __init__(self, simulation, worker_id):
        self.simulation = simulation
        self.worker_id = worker_id

    def run(self, current_time):
        return self.simulation.add_token(self.worker_id, current_time)


class TaskArrival(Event):
    """ Event to signify a task arriving at a worker. """
    def __init__(self, worker, task_duration, job_id):
        self.worker = worker
        self.task_duration = task_duration
        self.job_id = job_id

    def run(self, current_time):
        return self.worker.add_task(current_time, self.task_duration, self.job_id)

class TaskEndEvent():
    def __init__(self, worker):
        self.worker = worker

    def run(self, current_time):
        return self.worker.free_slot(current_time)

class Worker(object):
    def __init__(self, simulation, num_slots, id):
        self.simulation = simulation
        self.free_slots = num_slots
        # Just a list of (task duration, job id) pairs.
        self.queued_tasks = Queue.Queue()
        self.id = id
        self.worker_time_pairs = dict()

    def add_task(self, current_time, task_duration, job_id):
        #print "Task for job %s arrived at worker %s" % (job_id, self.id)
        self.queued_tasks.put((task_duration, job_id))
        return self.maybe_start_task(current_time)

    def free_slot(self, current_time):
        """ Frees a slot on the worker and attempts to launch another task in that slot. """
        self.free_slots += 1
        new_events = self.maybe_start_task(current_time)
        assert (self.free_slots > 0)
        if self.free_slots > 0:
            token = TokenArrival(self.simulation, self.id)
            new_events.append((current_time+NETWORK_DELAY, token))
        return new_events

    def maybe_start_task(self, current_time):
        if not self.queued_tasks.empty() and self.free_slots > 0:
            # Account for "running" task
            self.free_slots -= 1
            task_duration, job_id = self.queued_tasks.get()
            #print "Launching task for job %s on worker %s" % (job_id, self.id)
            task_end_time = current_time + task_duration
            #print ("Task for job %s on worker %s launched at %s; will complete at %s" %
            #(job_id, self.id, current_time, task_end_time))
            self.simulation.add_task_completion_time(job_id, task_end_time)
            return [(task_end_time, TaskEndEvent(self))]
        return []

class Simulation(object):
    class TaskHeader(object):
        def __init__(self, curr_queue_len):
            self.curr_queue_len = curr_queue_len

    def __init__(self, num_jobs, file_prefix, load, task_distribution):
        #util.NUM_SCHEDULERS = 2
        avg_used_slots = load * SLOTS_PER_WORKER * TOTAL_WORKERS
        self.interarrival_delay = (1.0 * MEDIAN_TASK_DURATION * TASKS_PER_JOB / avg_used_slots)
        print ("Interarrival delay: %s (avg slots in use: %s)" %
               (self.interarrival_delay, avg_used_slots))
        self.jobs = {}
        self.remaining_jobs = num_jobs
        self.finished_jobs = 0
        self.event_queue = Queue.PriorityQueue()
        self.workers = []
        self.unscheduled_jobs = []
        self.no_more_jobs = False
        for i in range(NUM_SCHEDULERS):
            self.unscheduled_jobs.append(Queue.PriorityQueue())
        self.file_prefix = file_prefix
        while len(self.workers) < TOTAL_WORKERS:
            self.workers.append(Worker(self, SLOTS_PER_WORKER, len(self.workers)))
        self.worker_indices = range(TOTAL_WORKERS)
        self.tokens = []
        for i in range(NUM_SCHEDULERS):
            self.tokens.append(list())
        #Populate initial tokens
        for i in range(TOTAL_WORKERS):
            for n in range(SLOTS_PER_WORKER):
                scheduler = random.randint(0, NUM_SCHEDULERS-1)
                self.tokens[scheduler].append(i)
        self.task_distribution = task_distribution

    def add_token(self, worker_id, current_time):
        scheduler = random.randint(0, NUM_SCHEDULERS-1)
        self.tokens[scheduler].append(worker_id)
        return self.maybe_schedule_task(current_time, scheduler)
       
    def steal_tokens(self, scheduler, tokens_needed):
        for i in range(NUM_SCHEDULERS):
            if(i != scheduler):
                i_tokens = len(self.tokens[i])
                while(i_tokens > 0 and tokens_needed > 0):
                    self.tokens[scheduler].append(self.tokens[i].pop())
                    i_tokens -= 1
                    tokens_needed -= 1

    def redistribute_tokens(self, current_time):
        tokens_needed = [0 for x in range(NUM_SCHEDULERS)]
        for scheduler in range(NUM_SCHEDULERS):
            for time, job in self.unscheduled_jobs[scheduler].queue:
                tokens_needed[scheduler] += len(job.unscheduled_tasks)
        schedulers = range(NUM_SCHEDULERS)
        random.shuffle(schedulers)
        new_events = []
        for i in range(NUM_SCHEDULERS):
            s_i = schedulers[i]
            for j in range(i+1, NUM_SCHEDULERS):
                s_j = schedulers[j]
                if(tokens_needed[s_i] > 0 and len(self.tokens[s_i]) > 0):
                    print "what the ", s_i, tokens_needed[s_i], len(self.tokens[s_i])
                assert(tokens_needed[s_i] == 0 or len(self.tokens[s_i]) == 0)
                while(tokens_needed[s_i] > 0 and len(self.tokens[s_j]) > 0):
                    self.tokens[s_i].append(self.tokens[s_j].pop())
                    new_events.extend(self.maybe_schedule_task(current_time + 2 * NETWORK_DELAY, s_i))
                    tokens_needed[s_i] -= 1
        return new_events 

    def send_tasks(self, job, current_time):
        """ Schedule tasks if tokens available. """
        self.jobs[job.id] = job
        scheduler = job.scheduler
        self.unscheduled_jobs[scheduler].put((current_time, job))
        '''
        if(len(self.tokens[scheduler]) < len(job.unscheduled_tasks)):
            #Insufficient tokens, steal tokens
            tokens_needed = len(job.unscheduled_tasks) - len(self.tokens[scheduler])
            self.steal_tokens(scheduler, tokens_needed)
            #Add network delay for token stealing
            current_time += 2 * NETWORK_DELAY
        '''
        #print "comes here", len(job.unscheduled_tasks), len(self.tokens[scheduler])
        task_arrival_events = self.maybe_schedule_task(current_time, scheduler)
        #print "finished"
        return task_arrival_events
                    
    def maybe_schedule_task(self, current_time, scheduler):
        task_arrival_events = []
        if(self.unscheduled_jobs[scheduler].qsize() > 0):
            #Only need to check more than 1 job, as function gets invoked on arrival of each token
            time_job, job = self.unscheduled_jobs[scheduler].get()
            ntasks = min(len(self.tokens[scheduler]), len(job.unscheduled_tasks))
            for i in range(ntasks):
                chosen_worker_idx = self.tokens[scheduler].pop()
                task_duration = job.unscheduled_tasks.pop()
                task_arrival_events.append(
                        (current_time + NETWORK_DELAY,
                         TaskArrival(self.workers[chosen_worker_idx], task_duration, job.id)))
            #If tasks left, put it back in the queue
            if(len(job.unscheduled_tasks) > 0):
                self.unscheduled_jobs[scheduler].put((time_job, job))
        return task_arrival_events


    def add_task_completion_time(self, job_id, completion_time):
        job_complete = self.jobs[job_id].task_completed(completion_time)
        if job_complete:
            self.remaining_jobs -= 1
            self.finished_jobs += 1

    def run(self):
        #self.event_queue.put((0, JobArrival(self, self.interarrival_delay, self.task_distribution)))
        job_arrival_event = JobArrivalFile(self, "/Users/vipulharsh/Desktop/UIUC/courses/cs525/eagle/traces/YH_trimmed.tr")
        last_time = 0
        last_time = job_arrival_event.job_generator.get_next_job_delay(last_time)
        self.event_queue.put((last_time, job_arrival_event))
        self.event_queue.put((last_time, HeartbeatEvent(self)))
        while self.remaining_jobs > 0 and (not (self.no_more_jobs and self.finished_jobs == len(self.jobs))):
            current_time, event = self.event_queue.get()
            assert current_time >= last_time
            last_time = current_time
            new_events = event.run(current_time)
            for new_event in new_events:
                self.event_queue.put(new_event)

        for i in range(NUM_SCHEDULERS):
            print "Leftover tokens at ",i,": ", len(self.tokens[i])
        print ("Simulation ended after %s milliseconds (%s jobs started)" %
               (last_time, len(self.jobs)))
        complete_jobs = [j for j in self.jobs.values() if j.completed_tasks_count == j.num_tasks]
        print "%s complete jobs" % len(complete_jobs)
        response_times = [job.end_time - job.start_time for job in complete_jobs
                          if job.start_time > WARM_UP_TIME]
        print "Included %s jobs" % len(response_times)
        plot_cdf(response_times, "%s_response_times.data" % self.file_prefix)
        print "Average response time: ", numpy.mean(response_times)

        longest_tasks = [job.longest_task for job in complete_jobs]
        plot_cdf(longest_tasks, "%s_ideal_response_time.data" % self.file_prefix)
        return response_times

def main():
    sim = Simulation(10000, "", 0.9, TaskDistributions.CONSTANT)
    sim.run()

if __name__ == "__main__":
    main()
