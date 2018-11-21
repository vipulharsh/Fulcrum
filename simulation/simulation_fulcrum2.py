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
from util import Job, TaskDistributions

MEDIAN_TASK_DURATION = 100
NETWORK_DELAY = 0.5
TASKS_PER_JOB = 100
SLOTS_PER_WORKER = 4
TOTAL_WORKERS = 10000

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
        self.simulation = simulation
        self.interarrival_delay = interarrival_delay
        self.task_distribution = task_distribution

    def run(self, current_time):
        job = Job(TASKS_PER_JOB, current_time, self.task_distribution, MEDIAN_TASK_DURATION)
        #print "Job %s arrived at %s" % (job.id, current_time)
        # Schedule job.
        new_events = self.simulation.send_tasks(job, current_time)
        # Add new Job Arrival event, for the next job to arrive after this one.
        arrival_delay = random.expovariate(1.0 / self.interarrival_delay)
        new_events.append((current_time + arrival_delay, self))
        #print "Retuning %s events" % len(new_events)
        return new_events

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
        self.last_token_sent_time = -1000000

    def recv_gossip_info(self, source_worker, gossip_info):
        for (w, t) in gossip_info:
            self.worker_time_pairs[w] = t

    def get_random_worker(self, current_time):
        workers = self.worker_time_pairs.keys()
        threshold = 100
        for w in workers:
            t = self.worker_time_pairs[w]
            #Filter older tokens
            if(current_time - t > threshold):
                del self.worker_time_pairs[w]
        if(len(self.worker_time_pairs) > 0):
            w = random.choice(self.worker_time_pairs.keys())
            del self.worker_time_pairs[w]
            return w
        else:
            return random.choice(self.simulation.worker_indices)

    def get_gossip_info(self, current_time):
        ret = []
        threshold = 100
        workers = self.worker_time_pairs.keys()
        for w in workers:
            t = self.worker_time_pairs[w]
            if(current_time - t <= threshold):
                if(random.randint(1, 10) == 7):
                    ret.append((w, t))
                    del self.worker_time_pairs[w]
            else:
                del self.worker_time_pairs[w]
        if(self.queued_tasks.qsize() == 0 and current_time - self.last_token_sent_time > 10): 
            #!TODO: Check if self.id is same as worker_index
            ret.append((self.id, current_time))
            self.last_token_sent_time = current_time
        return ret

    def add_task(self, current_time, task_duration, job_id):
        #print "Task for job %s arrived at worker %s" % (job_id, self.id)
        self.queued_tasks.put((task_duration, job_id))
        return self.maybe_start_task(current_time)

    def free_slot(self, current_time):
        """ Frees a slot on the worker and attempts to launch another task in that slot. """
        self.free_slots += 1
        get_task_events = self.maybe_start_task(current_time)
        return get_task_events

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
        avg_used_slots = load * SLOTS_PER_WORKER * TOTAL_WORKERS
        self.interarrival_delay = (1.0 * MEDIAN_TASK_DURATION * TASKS_PER_JOB / avg_used_slots)
        print ("Interarrival delay: %s (avg slots in use: %s)" %
               (self.interarrival_delay, avg_used_slots))
        self.jobs = {}
        self.remaining_jobs = num_jobs
        self.event_queue = Queue.PriorityQueue()
        self.workers = []
        self.file_prefix = file_prefix
        while len(self.workers) < TOTAL_WORKERS:
            self.workers.append(Worker(self, SLOTS_PER_WORKER, len(self.workers)))
        self.worker_indices = range(TOTAL_WORKERS)
        self.task_distribution = task_distribution


    def accept_task(self, task_header, worker_idx, k):
        if(k >= 9):
            return True
        queue_len = self.workers[worker_idx].queued_tasks.qsize() + SLOTS_PER_WORKER - self.workers[worker_idx].free_slots
        if((queue_len <= task_header.curr_queue_len and k>=4) or queue_len == 0):
            return True
        else:
            return False
        #if(queue_len > 0):
        #    print "Assigning task %s to worker %s at hop %s queue len %s" % (i, chosen_worker_idx, k, queue_len)
        

    def send_tasks(self, job, current_time):
        """ Task randomly hops on to other machines. """
        self.jobs[job.id] = job
        task_arrival_events = []
        for i in range(len(job.unscheduled_tasks)):
            delay = 0.0
            curr_header = self.TaskHeader(10000000) #task_header
            chosen_worker_idx = -1
            prev_worker = (job.job_id_hash)%len(self.workers)
            for k in range(10):
                delay += NETWORK_DELAY
                worker_idx = self.workers[prev_worker].get_random_worker(current_time)
                gossip_info = self.workers[prev_worker].get_gossip_info(current_time)
                self.workers[worker_idx].recv_gossip_info(prev_worker, gossip_info)
                if(self.accept_task(curr_header, worker_idx, k)):
                    chosen_worker_idx = worker_idx
                    break
                #Update Task Header
                queue_len = self.workers[worker_idx].queued_tasks.qsize() + SLOTS_PER_WORKER - self.workers[worker_idx].free_slots
                curr_header.curr_queue_len = min(queue_len, curr_header.curr_queue_len)
                #Update prev_worker
                prev_worker = worker_idx

            task_arrival_events.append(
                    (current_time + delay,
                     TaskArrival(self.workers[chosen_worker_idx], job.unscheduled_tasks[i], job.id)))
        return task_arrival_events
                    

    def add_task_completion_time(self, job_id, completion_time):
        job_complete = self.jobs[job_id].task_completed(completion_time)
        if job_complete:
            self.remaining_jobs -= 1

    def run(self):
        self.event_queue.put((0, JobArrival(self, self.interarrival_delay, self.task_distribution)))
        last_time = 0
        while self.remaining_jobs > 0:
            current_time, event = self.event_queue.get()
            assert current_time >= last_time
            last_time = current_time
            new_events = event.run(current_time)
            for new_event in new_events:
                self.event_queue.put(new_event)

        print ("Simulation ended after %s milliseconds (%s jobs started)" %
               (last_time, len(self.jobs)))
        complete_jobs = [j for j in self.jobs.values() if j.completed_tasks_count == j.num_tasks]
        print "%s complete jobs" % len(complete_jobs)
        response_times = [job.end_time - job.start_time for job in complete_jobs
                          if job.start_time > 500]
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
