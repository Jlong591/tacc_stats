"""GETS JOB INFO AND MOVES IT TO TACC-STATS
	-t  [--time: allows input of time to get past information
		     Default value is todays date]"""
#imports
import job
import sge_acct
import shelve
import time
import datetime
import os
import pickle
import getopt
import sys
import subprocess

#amount of ms in one day (for time stamps)
ONEDAY=86400
#path to where jobs stored on staging box
TACC_STATS_PATH="jlong591@tacc-stats:/home/jlong591/job-data"
#tmp path for symlinks
TMP_PATH="/tmp/TS"
#where the jobs are written to
RANGER_WORK_PATH="/work/01902/jlong591/TS"
JOB_PATH=RANGER_WORK_PATH
#accounting, archive, and hostlist paths for symlinks
ACCOUNTING_SCRATCH_PATH="/scratch/projects/tacc_stats/accounting"
ARCHIVE_SCRATCH_PATH="/scratch/projects/tacc_stats/archive"
HOSTLIST_SCRATCH_PATH="/scratch/projects/tacc_stats/hostfiles"
ACCT_SYM=os.path.join(TMP_PATH,"accounting")
ARCH_SYM=os.path.join(TMP_PATH,"stats")
HOSTS_SYM=os.path.join(TMP_PATH,"prolog_host_lists")

def main():
 #parse command line options
 try:
  opts, args = getopt.getopt(sys.argv[1:],'t:')
 except getopt.error, msg:
  print msg
  print "No time given, using today's date"
  time=time.mktime(datetime.date.today().timetuple())
 #process options
 for o, a in opts:
  if o in ("-t","--time"):
   time=long(a)
  makeDirs(time)
  moveJobs(time)

def makeDirs(time):
 #remove if anything stored in tmp to save space
 if not(os.path.exists(TMP_PATH)):
 #make working dirs and symlinks
  print "Creating TMP path"
  subprocess.call(["mkdir",TMP_PATH])
 if not(os.path.exists(RANGER_WORK_PATH)):
  print "Creating WORK path"
  subprocess.call(["mkdir",RANGER_WORK_PATH])
 global JOB_PATH
 JOB_PATH=os.path.join(JOB_PATH,str(long(time)))
 if not(os.path.exists(JOB_PATH)):
  print "Creating JOBPATH"
  subprocess.call(["mkdir",JOB_PATH])
 print "Creating SYMLINKS"
 subprocess.call(["ln","-s",ACCOUNTING_SCRATCH_PATH,ACCT_SYM])
 subprocess.call(["ln","-s",ARCHIVE_SCRATCH_PATH,ARCH_SYM])
 subprocess.call(["ln","-s",HOSTLIST_SCRATCH_PATH,HOSTS_SYM])

def moveJobs(time):
#get start_time
 global JOB_PATH
 global TACC_STATS_PATH
 global RANGER_WORK_PATH
 start_time=long(time)
 end_time=start_time + ONEDAY

#open job archive and put in list
 f = open(job.sge_acct_path)
 rd = sge_acct.reader(f, start_time=start_time, end_time=end_time)
 print "Making list: Start time = " +str(datetime.datetime.fromtimestamp(start_time))+"..."
 lis = list(rd)
 f.close()
#pickle every job
 for acct in lis:
  try:
   j=job.from_acct(acct)
  except TypeError:
   log(acct)
  #TODO: Handle this better...just skips job if it can't get the job
  #thisPath=os.path.join([JOB_PATH,str(j.id)+".pkl"])
  thisPath=JOB_PATH+"/"+str(j.id)+".pkl"
  f=open(thisPath,'wb')
  pickle.dump(j,f,-1)
  f.close()
#compress dir containing .pkl files
 subprocess.call(["tar","zcvf",RANGER_WORK_PATH+"/"+str(start_time)+".tar.gz","-C",RANGER_WORK_PATH,str(start_time)])
#transfer to tacc-stats
 subprocess.call(["scp",RANGER_WORK_PATH+"/"+str(start_time)+".tar.gz",TACC_STATS_PATH+"/"+str(start_time)+"/"])
#remove data from /work
 subprocess.call(["mv",RANGER_WORK_PATH+"/"+str(start_time)+".tar.gz",RANGER_WORK_PATH+"/"+str(start_time)+"/"])
 subprocess.call(["rm","-rf",RANGER_WORK_PATH+"/"+str(start_time)])

def log(acct):
 global JOB_PATH
 error=open(JOB_PATH+"/errors",'a')
 error.write("Errors processing job with account: " + str(acct)+"\n")
 error.close()

if __name__ == "__main__":
 main()
