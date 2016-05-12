from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool

from pyspark import SparkConf
from pyspark import SparkContext
from operator import add
import time
import random

conf = SparkConf()
conf.setAppName("chen")
conf.set("spark.scheduler.mode","GPS")

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
    SubmittingStandardInterval1 = 1000
    SubmittingStandardInterval2 = 2000
    cT = [0]
    TaskRunTimes = []
    JobSizes = []
    for i in range(NoJ):
        TaskRunTimes.append(random.randint(3000,5000))
        JobSizes.append(random.randint(5,5))
        cT.append(cT[-1] + TaskRunTimes[-1]*Sizes[-1])
    for i in range(NoJ-1):
	if i < 20:
       	    SubmittingTime.append(SubmittingTime[-1] + int(random.random()*SubmittingStandardInterval1))
    	else:
       	    SubmittingTime.append(SubmittingTime[-1] + int(random.random()*SubmittingStandardInterval2))
    for i in range(NoJ):
        if i==2:
#        if i%10==2:
#            Parameters.append([i, SubmittingTime[i], random.randint(11000,15000), random.randint(11, 40)])
            Parameters.append([i, SubmittingTime[i], random.randint(6000,6000), random.randint(20, 20)])
        else:
            Parameters.append([i, SubmittingTime[i], random.randint(1000,5000), random.randint(1,10)]) 
    return Parameters

if __name__=="__main__":
    NoJ = 200
    pool = ThreadPool(NoJ)
    parameters = set_parameters(NoJ)

    jobProperties = ""

    for i in range(NoJ):
	if i > 0:
	    jobProperties = jobProperties + " "
	jobProperties = jobProperties + str(parameters[i][0]) + "+" + str(parameters[i][1]) + "+" + str(parameters[i][2]*parameters[i][3])

    print "jobProperties: "+jobProperties

    pool.map(run_job, parameters)
    pool.close()
    pool.join()
