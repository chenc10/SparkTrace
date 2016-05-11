from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool

from pyspark import SparkConf
from pyspark import SparkContext
from operator import add
import time
import random

conf = SparkConf()
conf.setAppName("chen")
conf.set("spark.scheduler.mode","FAIR")

sc = SparkContext(conf=conf)
#    logData = sc.textFile(logFile).cache()

def waiting(wait_time):
    # milliseconds to wait
    start = time.time()
    while time.time()*1000 < start*1000 + wait_time:
        time.sleep(0.01)

def wait_map(wait_time, rdd):
    waiting(wait_time)
    return rdd

def run_job(P):
    partitionNum = 20
    waiting(P[1])
#    print "run-job-jobProperties: "+jobProperties
#    sc.setLocalProperty("job.profiledInfo", jobProperties)
    sc.setLocalProperty("spark.scheduler.pool", str(P[0]))
    value = sc.parallelize(range(partitionNum), partitionNum).map(lambda i: (i, i)).map(lambda i: wait_map(P[2], i))
    value.collect()
    print "job-%d finishes" %P[0]

if __name__=="__main__":
    NoJ = 110
    pool = ThreadPool(NoJ)
    
    # create parameters: (Id, submitting_time, run_time)
    Ids = range(NoJ)
#    SubmittingTime = range(NoJ)
    random.seed(0)
    submittingSlot = 500000
    SubmittingTime = [int(random.random()*submittingSlot) for i in range(NoJ)]
    SubmittingTime.sort()
    #Durations might be random
    durationPool = [8, 9, 10, 11, 12]
    Durations = [ durationPool[i%5]*1000 for i in range(NoJ)]
    for i in range(10):
	Durations[i*10+5] = 1000

    jobProperties = ""

    parameters = []
    for i in range(NoJ):
	parameters.append([Ids[i], SubmittingTime[i], Durations[i]])
	if i > 0:
	    jobProperties = jobProperties + " "
	jobProperties = jobProperties + str(Ids[i]) + "+" + str(SubmittingTime[i]) + "+" + str(Durations[i])

    print "jobProperties: "+jobProperties

    pool.map(run_job, parameters)
    pool.close()
    pool.join()
