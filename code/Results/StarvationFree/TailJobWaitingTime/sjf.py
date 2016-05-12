from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool

from pyspark import SparkConf
from pyspark import SparkContext
from operator import add
import time
import random

conf = SparkConf()
conf.setAppName("chen")
conf.set("spark.scheduler.mode","SJF")

sc = SparkContext(conf=conf)
#    logData = sc.textFile(logFile).cache()

#jobProperties = "0+0+40 1+20+100 2+30+20"
#sc.setLocalProperty("stage.profiledInfo", "0+0+40 1+0+40 1+1+40 1+2+40 1+3+20 2+0+20")

def waiting(wait_time):
    # milliseconds to wait
    start = time.time()
    while time.time()*1000 < start*1000 + wait_time/10:
        time.sleep(0.01)

def wait_map(wait_time, rdd):
    waiting(wait_time)
    return rdd

def run_job(P):
    partitionNum = P[3]
    waiting(P[1])
    print "run-job-jobProperties: "+jobProperties
    sc.setLocalProperty("job.profiledInfo", jobProperties)
    value = sc.parallelize(range(partitionNum), partitionNum).map(lambda i: (i, i)).map(lambda i: wait_map(P[2], i))
    value.collect()
    print "job-%d finishes" %P[0]

def set_parameters(NoJ):
    random.seed(0)
    Parameters = []
    SubmittingTime = [2000]
    SubmittingStandardInterval1 = 500
    SubmittingStandardInterval2 = 6000
    cT = [0]
    TaskRunTimes = []
    JobSizes = []
    for i in range(NoJ):
#        TaskRunTimes.append(4000)
#        JobSizes.append(20)

        TaskRunTimes.append(random.randint(4000,5000))
        JobSizes.append(random.randint(17,20))

#        TaskRunTimes.append(random.randint(4000,4000))
#        JobSizes.append(random.randint(1,10))
#        cT.append(cT[-1] + TaskRunTimes[-1]*Sizes[-1])
    for i in range(NoJ-1):
	if i < 4:
       	    SubmittingTime.append(SubmittingTime[-1] + int(random.random()*SubmittingStandardInterval1))
    	else:
       	    SubmittingTime.append(SubmittingTime[-1] + int(random.random()*SubmittingStandardInterval2))
    for i in range(NoJ):
        if i==2000:
#        if i%10==2:
#            Parameters.append([i, SubmittingTime[i], random.randint(11000,15000), random.randint(11, 40)])
#            Parameters.append([i, SubmittingTime[i], random.randint(6000,6000), random.randint(20, 20)])
            Parameters.append([i, SubmittingTime[i], 5000, 20]) 
        else:
            Parameters.append([i, SubmittingTime[i], TaskRunTimes[i], JobSizes[i]])
#            Parameters.append([i, SubmittingTime[i], random.randint(1000,5000), random.randint(1,10)]) 
#            Parameters.append([i, SubmittingTime[i], TaskRunTimes[i], JobSizes[i]])
    return Parameters

if __name__=="__main__":
    NoJ = 200
    pool = ThreadPool(NoJ)
    parameters = set_parameters(NoJ)

    jobProperties = ""

    for i in range(NoJ):
	if i > 0:
	    jobProperties = jobProperties + " "
	jobProperties = jobProperties + str(parameters[i][0]) + "+" + str(parameters[i][1]) + "+" + str(int(parameters[i][2]*parameters[i][3]/20.0))

    print "jobProperties: "+jobProperties
    f=file("/root/spark/job.profiledInfo","w")
    f.write(jobProperties)

    pool.map(run_job, parameters)
    pool.close()
    pool.join()
