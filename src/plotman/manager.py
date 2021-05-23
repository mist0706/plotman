import logging
import operator
import os
import random
import re
import readline  # For nice CLI
import subprocess
import sys
import threading
import time
from datetime import datetime
import requests
from discord import Webhook, RequestsWebhookAdapter
import redis

import psutil

# Plotman libraries
from plotman import \
    archive  # for get_archdir_freebytes(). TODO: move to avoid import loop
from plotman import job, plot_util, configuration

# Constants
MIN = 60    # Seconds
HR = 3600   # Seconds

MAX_AGE = 1000_000_000   # Arbitrary large number of seconds

cfg = configuration.get_validated_configs()
webhook_url = cfg.distribution.webhook_url
webhook = Webhook.from_url(webhook_url, adapter=RequestsWebhookAdapter())


def dstdirs_to_furthest_phase(all_jobs):
    '''Return a map from dst dir to a phase tuple for the most progressed job
       that is emitting to that dst dir.'''
    result = {}
    for j in all_jobs:
        if not j.dstdir in result.keys() or result[j.dstdir] < j.progress():
            result[j.dstdir] = j.progress()
    return result

def dstdirs_to_youngest_phase(all_jobs):
    '''Return a map from dst dir to a phase tuple for the least progressed job
       that is emitting to that dst dir.'''
    result = {}
    for j in all_jobs:
        if not j.dstdir in result.keys() or result[j.dstdir] > j.progress():
            result[j.dstdir] = j.progress()
    return result

def phases_permit_new_job(phases, d, sched_cfg, dir_cfg):
    '''Scheduling logic: return True if it's OK to start a new job on a tmp dir
       with existing jobs in the provided phases.'''
    # Filter unknown-phase jobs
    phases = [ph for ph in phases if ph[0] is not None and ph[1] is not None]

    if len(phases) == 0:
        return True

    milestone = (sched_cfg.tmpdir_stagger_phase_major, sched_cfg.tmpdir_stagger_phase_minor)
    # tmpdir_stagger_phase_limit default is 1, as declared in configuration.py
    if len([p for p in phases if p < milestone]) >= sched_cfg.tmpdir_stagger_phase_limit:
        return False

    # Limit the total number of jobs per tmp dir. Default to the overall max
    # jobs configuration, but restrict to any configured overrides.
    max_plots = sched_cfg.tmpdir_max_jobs
    if dir_cfg.tmp_overrides is not None and d in dir_cfg.tmp_overrides:
        curr_overrides = dir_cfg.tmp_overrides[d]
        if curr_overrides.tmpdir_max_jobs is not None:
            max_plots = curr_overrides.tmpdir_max_jobs
    if len(phases) >= max_plots:
        return False

    return True

def maybe_start_new_plot(dir_cfg, sched_cfg, plotting_cfg):
    jobs = job.Job.get_running_jobs(dir_cfg.log)

    wait_reason = None  # If we don't start a job this iteration, this says why.

    youngest_job_age = min(jobs, key=job.Job.get_time_wall).get_time_wall() if jobs else MAX_AGE
    global_stagger = int(sched_cfg.global_stagger_m * MIN)
    if (youngest_job_age < global_stagger):
        wait_reason = 'stagger (%ds/%ds)' % (youngest_job_age, global_stagger)
    elif len(jobs) >= sched_cfg.global_max_jobs:
        wait_reason = 'max jobs (%d)' % sched_cfg.global_max_jobs
    else:
        tmp_to_all_phases = [(d, job.job_phases_for_tmpdir(d, jobs)) for d in dir_cfg.tmp]
        eligible = [ (d, phases) for (d, phases) in tmp_to_all_phases
                if phases_permit_new_job(phases, d, sched_cfg, dir_cfg) ]
        rankable = [ (d, phases[0]) if phases else (d, (999, 999))
                for (d, phases) in eligible ]
        
        if not eligible:
            wait_reason = 'no eligible tempdirs'
        else:
            # Plot to oldest tmpdir.
            tmpdir = max(rankable, key=operator.itemgetter(1))[0]

            # Select the dst dir least recently selected
            dir2ph = { d:ph for (d, ph) in dstdirs_to_youngest_phase(jobs).items()
                      if d in dir_cfg.dst }
            unused_dirs = [d for d in dir_cfg.dst if d not in dir2ph.keys()]
            dstdir = ''
            if unused_dirs: 
                dstdir = random.choice(unused_dirs)
            else:
                dstdir = max(dir2ph, key=dir2ph.get)

            logfile = os.path.join(
                dir_cfg.log, datetime.now().strftime('%Y-%m-%d-%H:%M:%S.log')
            )

            # Check for jobs from the scheduler
            redis_jobs = redis.Redis(host=cfg.distribution.redis_host, port=6379, db=cfg.distribution.redis_jobs, decode_responses=True, charset="utf-8", password=cfg.distribution.redis_pass)
            redis_plots = redis.Redis(host=cfg.distribution.redis_host, port=6379, db=cfg.distribution.redis_plots, decode_responses=True, charset="utf-8", password=cfg.distribution.redis_pass)
            customer_jobs = redis_jobs.keys()
            customer_jobs = [int(custid) for custid in customer_jobs]
            customer_jobs.sort()
            is_customerjob = False
            plot_args = create_plot_command(plotting_cfg, tmpdir, dstdir, dir_cfg)
            logmsg = "Default value, something went wrong"
            if len(customer_jobs) > 0:
                for s_job in customer_jobs:
                    job_data = redis_jobs.hgetall(s_job)
                    if int(job_data['plots_scheduled']) < int(job_data['plots_ordered']):
                        plotting_cfg.farmer_pk = job_data['farmer_pk']
                        plotting_cfg.pool_pk = job_data['pool_pk']
                        plotting_cfg.k = job_data.get('size', '32')
                        plots_scheduled = int(job_data['plots_scheduled']) + 1
                        redis_jobs.hset(s_job, 'plots_scheduled', plots_scheduled)
                        plot_args = create_plot_command(plotting_cfg, tmpdir, dstdir, dir_cfg)
                        p = call_subprocess(plot_args, logfile)

                        plotid = get_plotid(logfile)

                        # Save the plot id for easy overview and archiving 
                        redis_plots.hset(s_job, plotid, logfile)
                        logmsg = ('Starting job for farmkey: %s ; plotid %s ; job (%s/%s)' % (plot_args[16][:8], plotid, plots_scheduled, job_data['plots_ordered']))
                        webhook.send(logmsg)
                        break

                    elif job_data['plots_ordered'] == job_data['plots_scheduled']:
                        plots_scheduled = int(job_data['plots_scheduled']) + 1
                        redis_jobs.hset(s_job, 'plots_scheduled', plots_scheduled)
                        webhook.send("All ordered plots have been scheduled for customer id: %s" % str(s_job))
                        logmsg = ('Looped through all orders for current job, completed')
                        break
            else:
                plotid = get_plotid(logfile)
                p = call_subprocess(plot_args, logfile)
                logmsg = ('Starting private plot ; plotid %s ; logging to %s' % (plotid, logfile))

            return (True, logmsg)

    return (False, wait_reason)

def get_plotid(logfile):
    # Figure out which plot id belongs to the customer
    time.sleep(5) # Wait 5 seconds to make sure that the process has spun up and forked
    command = "head %s | fgrep ID | cut -d ' ' -f2 | cut -b 1-8" % logfile
    out = subprocess.run(command, capture_output=True, shell=True)
    plotid = out.stdout.strip().decode("utf-8") 
    return plotid

def call_subprocess(plot_args, logfile):
    # start_new_sessions to make the job independent of this controlling tty.
    p = subprocess.Popen(plot_args,
        stdout=open(logfile, 'w'),
        stderr=subprocess.STDOUT,
        start_new_session=True)
    psutil.Process(p.pid).nice(15)
    return p


def create_plot_command(plotting_cfg, tmpdir, dstdir, dir_cfg):

    plot_args = ['chia', 'plots', 'create',
            '-k', str(plotting_cfg.k),
            '-r', str(plotting_cfg.n_threads),
            '-u', str(plotting_cfg.n_buckets),
            '-b', str(plotting_cfg.job_buffer),
            '-t', tmpdir,
            '-d', dstdir ]
    if plotting_cfg.e:
        plot_args.append('-e')
    if plotting_cfg.farmer_pk is not None:
        plot_args.append('-f')
        plot_args.append(plotting_cfg.farmer_pk)
    if plotting_cfg.pool_pk is not None:
        plot_args.append('-p')
        plot_args.append(plotting_cfg.pool_pk)
    if dir_cfg.tmp2 is not None:
        plot_args.append('-2')
        plot_args.append(dir_cfg.tmp2)
    return plot_args


def select_jobs_by_partial_id(jobs, partial_id):
    selected = []
    for j in jobs:
        if j.plot_id.startswith(partial_id):
            selected.append(j)
    return selected
