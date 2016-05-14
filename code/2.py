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
    while time.time()*1000 < start*1000 + wait_time:
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

def set_parameters(Data):
    random.seed(0)
    Parameters = []
    Intervals = []
    JobSizes = []
    TaskRunTimes = []
    for i in range(len(Data)):
        tmp = Data[i].split("\t")
        Intervals = int(tmp[2])
        JobSizes = int(tmp[3])
        TaskRunTimes = int(tmp[4])
    SubmittingTime = [200]
#    SubmittingStandardInterval1 = 300
#    SubmittingStandardInterval2 = 1000
#    cT = [0]
#    for i in range(NoJ):
#        TaskRunTimes.append(4000)
#        JobSizes.append(20)

#        TaskRunTimes.append(random.randint(800,1300))
#        JobSizes.append(random.randint(8,13))

#        TaskRunTimes.append(random.randint(4000,4000))
#        JobSizes.append(random.randint(1,10))
#        cT.append(cT[-1] + TaskRunTimes[-1]*Sizes[-1])
    for i in range(len(Data)-1):
       	SubmittingTime.append(SubmittingTime[-1] + Intervals[i+1])
    for i in range(len(Data)):
        Parameters.append([i, SubmittingTime[i], TaskRunTimes[i], JobSizes[i]])
#            Parameters.append([i, SubmittingTime[i], random.randint(1000,5000), random.randint(1,10)]) 
#            Parameters.append([i, SubmittingTime[i], TaskRunTimes[i], JobSizes[i]])
    return Parameters

if __name__=="__main__":
    f = open("small-scale-sampledData",'r')
    Data = f.readlines()
    f.close()
#    NoJ = 51
    pool = ThreadPool(len(Data))
    parameters = set_parameters(Data)
    NumOfSlots = 20.0

    jobProperties = ""

    for i in range(NoJ):
	if i > 0:
	    jobProperties = jobProperties + " "
	jobProperties = jobProperties + str(parameters[i][0]) + "+" + str(parameters[i][1]) + "+" + str(int(parameters[i][2]*parameters[i][3]/NumOfSlots))

    print "jobProperties: "+jobProperties
    f=file("/root/spark/job.profiledInfo","w")
    f.write(jobProperties)
    f.close()
    time.sleep(1)

    pool.map(run_job, parameters)
    pool.close()
    pool.join()
