"""updateJobs.py: 

Synopsis
--------
    updateJobs.py <optional option> [option argument]

Description
-----------
    Updates the postgres database for specified time or all jobs

Environment
-----------
    Like build_database.py, django needs to know the settings module. Use
    environment variable DJANGO_SETTINGS_MODULE to name the module. The module
    needs to be in the python path.

    EXAMPLE:
	     
    $export PYTHONPATH=<path_to_tacc_stats_web>:<path_to_tacc_stats_web/apps>:$PYTHONPATH
    $export DJANGO_SETTINGS_MODULE='tacc_stats_web.settings'

Option
------
    -t	--time:
	Following -t, insert epoch time stamp for day of jobs in job-data
        Optional option - without it, simply updates for all times in job-data

"""


import os
import datetime
import time
import pickle
import getopt
import sys
import subprocess
import commands

#database imports
from django.conf import settings
from django.core.management import call_command
from django.db.utils import DatabaseError
from django.db import transaction

sys.path.insert(0,'/home/jlong591/tstat/tacc_stats/monitor')

from tacc_stats.models import Node, System, User
from tacc_stats.models import Job as tsm_Job

import job
import sge_acct
from job import Job

JOB_PATH="/home/jlong591/job-data/"
LIST_OF_TIMES=[]

###SYSTME VARIABLE!! CHANGE FOR OTHER SYSTEMS!!!###
WORKING_SYSTEM='ranger.tacc.utexas.edu'

def main():
 """parse through options then call appropraite specified or all jobs function"""
 #first cd to job-data to get list of times
 global JOB_PATH
 global LIST_OF_TIMES
 #subprocess.call(['cd',JOB_PATH])
 LIST_OF_TIMES=commands.getoutput("ls "+JOB_PATH).split()
 #parse command line options
 try:
  opts, args = getopt.getopt(sys.argv[1:],'t:')
 except getopt.error, msg:
  print msg
  print "No time given, updating database for all days in job-data"
  allJobs()
 #process options
 for o, a in opts:
  if o in ("-t","--time"):
   if (str(a)+".tar.gz") in LIST_OF_TIMES:
    print "Processing jobs for " + str(a)
    time=long(a)
    specifiedJobs(time)
    backup(time)
   else:
    print "Time specified has no job data; exiting"

def specifiedJobs(time):
 """if a time is specified, process jobs for only that day"""
 global JOB_PATH
 #subprocess.call(['cd',JOB_PATH+"/"+str(time)])
 thispath=JOB_PATH
 #extract jobs from specified day
 subprocess.call(['tar','-C',thispath,'-xzvf',thispath+str(time)+".tar.gz"])
 #phew, we made it. now get all the jobs in a list
 thispath=thispath+str(time)
 job_list=commands.getoutput("ls "+thispath).split() #split ignores '\n'
 processJobs(thispath,job_list)
  


def allJobs():
 #if time not specified, update postgres for ALL JOBS IN ~/job-data
 global JOB_PATH
 global LIST_OF_TIMES
 for time in LIST_OF_TIMES:
  workingPath=JOB_PATH
  try:
   subprocess.call(['tar','-C',workingPath,'-xzvf',workingPath+str(time)+".tar.gz"])
  except:
   print "No file to decompress. Trying next time"
  #might seem redundant, but the tar file extacts a dir named by the time
  workingPath=JOB_PATH+str(time).split('.')[0]
  job_list=commands.getoutput('ls '+workingPath).split()
  processJobs(workingPath,job_list)


def processJobs(path,job_list):
 """process the list of jobs and update postgres db for each"""
 call_command('syncdb')
 global WORKING_SYSTEM
 system=get_system(WORKING_SYSTEM)
 for j in job_list:
  #file errors contains error files, obv not going to update db
  if j =='"errors':
   continue
  try:
   f=open(path+"/"+j,'rb')
   a_job=pickle.load(f)
   f.close()
   #TODO: figure out if it's better to update all jobs individually or separate
   updateDatabase(system,a_job)
  except IOError:
   print "Error processing job"

def updateDatabase(system,a_job):
 if system.job_set.filter(acct_id=int(a_job.id)):
  print "Job %s already exists" % a_job.id
  return
 owner = get_user(a_job.acct['account'], system)
 #nr_bad_hosts = len(filter(lambda h: len(h.times) < 2,
 #                          a_job.hosts.values()))
 job_dict = {
  'system': system,
  'acct_id': a_job.id,
  'owner': owner,
  'queue': a_job.acct['queue'],
  'queue_wait_time': a_job.start_time - a_job.acct['submission_time'],
  'begin': a_job.start_time,
  'end': a_job.end_time,
  #'nr_bad_hots': nr_bad_hosts,
  'nr_slots': a_job.acct['slots'],
  'pe': a_job.acct['granted_pe'],
  'failed': a_job.acct['failed'],
  'exit_status': a_job.acct['exit_status'],
 }
 job_dict.update(job.JobAggregator(a_job).stats)
 #newJob.nr_hosts = len(a_job.hosts)
 try:
  newJob = tsm_Job(**job_dict)
  newJob.save()
 except DatabaseError,TypeError:
  print "Error on job,", a_job.id
  transaction.rollback()
  return
 hosts = map(lambda node: get_node(system, node), a_job.hosts.keys())
 newJob.hosts = hosts
 print "Added job:", a_job.id


def get_system(system_name):
 """Returns system, adding it if system if not in db."""
 systems = System.objects.filter(name=system_name)
 if len(systems) == 0:
  print "Adding system: %s" % system_name
  system = System(name=system_name)
  system.save()
 else:
  system = systems[0]
 return system

def get_user(user_name, system):
 """Returns the user cooresponding to the user_name, if it doesn't exist it is created"""
 users = User.objects.filter(user_name=user_name)
 if len(users) == 0:
  print "Adding user:", user_name
  user = User(user_name = user_name)
  user.save()
 else:
  user = users[0]
 user.systems.add(system)
 return user

def get_node(system, node_name):
 """Returns node, if it doesn't exist it is created"""
 node_name = strip_system_name(system.name, node_name)
 nodes = system.node_set.filter(name=node_name)
 if len(nodes) == 0:
  print "Adding node: %s" % node_name
  node = system.node_set.create(name=node_name)
 else:
  node = nodes[0]
 return node

def strip_system_name(system_name, node):
 """Removes the system name from a node"""
 if system_name in node:
  end = node.find(system_name)
  end = end - 1 if node[end-1] == '.' else end
  node = node[:end]
 return node

def backup(t):
 global JOB_PATH
 path=JOB_PATH+str(t)+".tar.gz"
 #note: need to fix keys with this
 subprocess.call(["scp",path,"jlong591@ranch.tacc.utexas.edu:/home/01902/jlong591/job-data"])

if __name__ == "__main__":
 main()
