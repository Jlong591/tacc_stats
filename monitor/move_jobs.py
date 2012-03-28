#GETS JOB INFO AND MOVES IT TO TACC-STATS
#	-t  [--time: allows input of time to get past information
#		     Default value is todays date]

import job, sge_acct, shelve, time, datetime, os, pickle, getopt, sys


#get time option for job-script loop
letters='t:'
keywords=['time=']

opts, extraparams = getopt.getopt(sys.argv[1:],letters)
#default time value is today at 00:00:00
time=time.mktime(datetime.date.today().timetuple())
for o,p in opts:
    if o in ['-t','--time']:
	time = p

#turn off all that annoying crap
job.TS_VERBOSE=False
#remove if anything stored in tmp to save space
os.system("rm -rf /tmp/TS")
#make working dirs and symlinks
os.system("mkdir /tmp/TS")
os.system("mkdir /work/01902/jlong591/TS")
os.system("mkdir /work/01902/jlong591/TS"+str(long(time)))
os.system("ln -s /scratch/projects/tacc_stats/accounting /tmp/TS/accounting")
os.system("ln -s /scratch/projects/tacc_stats/archive /tmp/TS/stats")
os.system("ln -s /scratch/projects/tacc_stats/hostfiles /tmp/TS/prolog_host_lists")

tacc_stats_path="jlong591@tacc-stats.tacc.utexas.edu:/job-data/"+str(long(time))
jobPath="/work/01902/jlong591/TS/"+str(long(time))


#get start_time
start_time=long(time)
end_time=start_time + 86400

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
	except:
		#TODO: Handle this better...just skips job if it can't get the job
		continue
	thisPath=jobPath+str(j.id)+".pkl"
	f=open(thisPath,'wb')
	pickle.dump(j,f,-1)
	f.close()
#compress dir containing .pkl files
os.system("tar -zcvf "+jobPath+str(start_time)+".tar.gz "+jobPath)
#transfer to tacc-stats
os.system("scp "+jobPath+str(start_time)+".tar.gz jlong591@tacc-stats:/job-data/"+str(start_time)+"/")
#remove data from /work
os.system("rm -rf /work/01902/jlong591/TS")
