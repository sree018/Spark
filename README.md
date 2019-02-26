 Spark is a computational engine, which is responsible for scheduling, distributed, and monitoring applications.
 Apache Spark consists of these components
 ## 1) Spark Core(RDD)
 ## 2) Spark SQL
 ## 3) Spark Streaming 
 ## 4) Spark Machine Learning
 ## 5) Spark GraphX

## Spark Workflow of Job Submission:
 ![alt text](https://spark.apache.org/docs/latest/img/cluster-overview.png)
  When client submit spark Job to Machine, in spark Context present, in driver program runs the main () function of the application. The driver converts code into a logical directed acyclic graph (DAG) and submits it to the DAG scheduler.

	DAGScheduler:
computes DAG of stages for each job and submits them to TaskScheduler determines preferred locations for tasks (based on cache status or shuffle files locations) and finds minimum schedule to run the jobs
	TaskScheduler:
responsible for sending tasks to the cluster, running them, retrying if there are failures, and mitigating stragglers
	SchedulerBackend:
backend interface for scheduling systems that allows plugging in different implementations (Mesos, YARN, Standalone, local)
	BlockManager:                                                        provides interfaces for putting and retrieving blocks both locally and remotely into various stores (memory, disk, and off-heap)
