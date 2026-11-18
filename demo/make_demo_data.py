#!/usr/bin/env python3
"""Generate the demo inputs: three Spark applications with different problems, as event logs plus executor
stderr, in demo/spark-events/<app id>/.

    python3 demo/make_demo_data.py            # writes demo/spark-events/

  application_1700000000000_0042  daily_sales_join        skewed join + containers killed (off-heap)
  application_1700000000000_0057  orders_enrichment       executors out of heap (java.lang.OutOfMemoryError)
  application_1700000000000_0063  clickstream_sessionize  healthy but 40 fat executors sitting mostly idle

Data quality demo rows are inserted by demo/seed_quality.sql.
"""
import json
import random
import sys
from pathlib import Path

MB = 1024 ** 2
GB = 1024 ** 3
START = 1_700_000_000_000


class EventLog:
    def __init__(self, app_id: str, app_name: str, spark_conf: dict, executors: int, cores: int):
        self.events = [
            {"Event": "SparkListenerLogStart", "Spark Version": "3.5.3"},
            {"Event": "SparkListenerEnvironmentUpdate", "JVM Information": {},
             "Spark Properties": {"spark.app.name": app_name, "spark.submit.deployMode": "cluster", **spark_conf}},
            {"Event": "SparkListenerApplicationStart", "App Name": app_name, "App ID": app_id, "Timestamp": START},
        ]
        self.executors = executors
        self.next_task_id = 0
        for i in range(executors):
            self.events.append({"Event": "SparkListenerExecutorAdded", "Timestamp": START + 5000,
                                "Executor ID": str(i + 1), "Executor Info": {"Host": f"worker-{i % 8}",
                                                                             "Total Cores": cores}})

    def plan(self, description: str):
        self.events.append({"Event": "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart",
                            "executionId": 0, "description": "save", "time": START + 6000,
                            "physicalPlanDescription": description})

    def stage(self, stage_id: int, name: str, tasks: int, start: int, duration_ms, metrics,
              heap_bytes: int, failed=lambda k: False) -> int:
        end = start
        for k in range(tasks):
            duration = duration_ms(k)
            launch = start + random.randint(0, 2000)
            reason = {"Reason": "ExceptionFailure", "Class Name": "java.lang.OutOfMemoryError",
                      "Description": "Java heap space"} if failed(k) else {"Reason": "Success"}
            self.events.append({
                "Event": "SparkListenerTaskEnd", "Stage ID": stage_id, "Stage Attempt ID": 0,
                "Task End Reason": reason,
                "Task Info": {"Task ID": self.next_task_id, "Executor ID": str(k % self.executors + 1),
                              "Launch Time": launch, "Finish Time": launch + duration, "Failed": failed(k),
                              "Killed": False},
                "Task Metrics": metrics(k, duration), "Task Executor Metrics": {"JVMHeapMemory": heap_bytes}})
            self.next_task_id += 1
            end = max(end, launch + duration)
        self.events.append({"Event": "SparkListenerStageCompleted", "Stage Info": {
            "Stage ID": stage_id, "Stage Attempt ID": 0, "Stage Name": name, "Number of Tasks": tasks,
            "Submission Time": start, "Completion Time": end}})
        return end

    def finish(self, end: int, succeeded: bool = True):
        self.events.append({"Event": "SparkListenerJobEnd", "Job ID": 0, "Completion Time": end,
                            "Job Result": {"Result": "JobSucceeded" if succeeded else "JobFailed"}})
        self.events.append({"Event": "SparkListenerApplicationEnd", "Timestamp": end + 2000})

    def write(self, directory: Path, stderr: str):
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "eventlog.json").write_text("\n".join(json.dumps(e) for e in self.events) + "\n")
        (directory / "executor-stderr.log").write_text(stderr)


def daily_sales_join(root: Path):
    """Skewed sort-merge join; one hot key partition; YARN kills a container for off-heap use."""
    log = EventLog("application_1700000000000_0042", "daily_sales_join",
                   {"spark.executor.memory": "8g", "spark.executor.cores": "8", "spark.executor.instances": "10",
                    "spark.driver.memory": "4g", "spark.sql.shuffle.partitions": "200",
                    "spark.sql.adaptive.enabled": "false"}, executors=10, cores=8)
    log.plan("== Physical Plan ==\n*(5) SortMergeJoin [customer_id], [customer_id], Inner\n"
             ":- Exchange hashpartitioning(customer_id, 200)\n+- FileScan parquet sales")
    scan_end = log.stage(0, "scan parquet sales", 480, START + 7000, lambda k: random.randint(20000, 40000),
                         lambda k, d: {"Executor Run Time": d, "Executor CPU Time": d * 700_000,
                                       "JVM GC Time": int(d * 0.06), "Input Metrics": {"Bytes Read": 128 * MB},
                                       "Shuffle Write Metrics": {"Shuffle Bytes Written": 100 * MB}},
                         heap_bytes=int(7.2 * GB))

    def join_metrics(k, d):
        shuffle_read = 9 * GB if k == 17 else 230 * MB
        return {"Executor Run Time": d, "Executor CPU Time": d * 500_000, "JVM GC Time": int(d * 0.22),
                "Memory Bytes Spilled": shuffle_read * 3, "Disk Bytes Spilled": shuffle_read // 2,
                "Peak Execution Memory": 600 * MB,
                "Shuffle Read Metrics": {"Remote Bytes Read": int(shuffle_read * 0.8),
                                         "Local Bytes Read": int(shuffle_read * 0.2), "Fetch Wait Time": int(d * 0.05)},
                "Shuffle Write Metrics": {"Shuffle Bytes Written": 20 * MB}}

    end = log.stage(1, "save at daily_sales_join.py:88", 200, scan_end + 500,
                    lambda k: 900_000 if k == 17 else random.randint(40000, 70000), join_metrics,
                    heap_bytes=int(7.2 * GB))
    log.events.append({"Event": "SparkListenerExecutorRemoved", "Timestamp": end - 300000, "Executor ID": "4",
                       "Removed Reason": "Container killed by YARN for exceeding physical memory limits. "
                                         "9.1 GB of 8.8 GB used."})
    log.finish(end)
    log.write(root / "application_1700000000000_0042", """\
26/11/18 02:12:11 INFO UnsafeExternalSorter: Thread 71 spilling sort data of 1824.0 MiB to disk (3 times so far)
26/11/18 02:19:40 WARN YarnAllocator: Container killed by YARN for exceeding physical memory limits. 9.1 GB of 8.8 GB physical memory used. Consider boosting spark.executor.memoryOverhead.
26/11/18 02:19:41 ERROR YarnClusterScheduler: Lost executor 4 on worker-3: Container killed by YARN for exceeding physical memory limits.
""")


def orders_enrichment(root: Path):
    """Wide rows aggregated per customer: tasks hold too much in the heap and die with OutOfMemoryError."""
    log = EventLog("application_1700000000000_0057", "orders_enrichment",
                   {"spark.executor.memory": "4g", "spark.executor.cores": "4", "spark.executor.instances": "6",
                    "spark.sql.shuffle.partitions": "64", "spark.sql.adaptive.enabled": "true"}, executors=6, cores=4)
    log.plan("== Physical Plan ==\nAdaptiveSparkPlan\n+- HashAggregate(keys=[customer_id], "
             "functions=[collect_list(order)])\n   +- Exchange hashpartitioning(customer_id, 64)")
    scan_end = log.stage(0, "scan kafka orders", 96, START + 7000, lambda k: random.randint(15000, 25000),
                         lambda k, d: {"Executor Run Time": d, "Executor CPU Time": d * 800_000,
                                       "JVM GC Time": int(d * 0.08), "Input Metrics": {"Bytes Read": 160 * MB},
                                       "Shuffle Write Metrics": {"Shuffle Bytes Written": 150 * MB}},
                         heap_bytes=int(3.1 * GB))
    end = log.stage(1, "collect_list orders by customer", 64, scan_end + 500, lambda k: random.randint(90000, 160000),
                    lambda k, d: {"Executor Run Time": d, "Executor CPU Time": d * 350_000,
                                  "JVM GC Time": int(d * 0.41), "Memory Bytes Spilled": 900 * MB,
                                  "Disk Bytes Spilled": 300 * MB, "Peak Execution Memory": 1400 * MB,
                                  "Shuffle Read Metrics": {"Remote Bytes Read": 180 * MB, "Local Bytes Read": 45 * MB,
                                                           "Fetch Wait Time": 400}},
                    heap_bytes=int(3.9 * GB), failed=lambda k: k % 9 == 0)
    log.finish(end, succeeded=False)
    log.write(root / "application_1700000000000_0057", """\
26/11/18 03:41:02 WARN TaskSetManager: Lost task 9.0 in stage 1.0 (TID 105): java.lang.OutOfMemoryError: Java heap space
26/11/18 03:41:02 ERROR Executor: Exception in task 9.0 in stage 1.0 (TID 105)
java.lang.OutOfMemoryError: Java heap space
	at java.base/java.util.Arrays.copyOf(Arrays.java:3537)
26/11/18 03:44:17 ERROR Executor: Exception in task 18.1 in stage 1.0 (TID 131)
java.lang.OutOfMemoryError: Java heap space
26/11/18 03:47:55 WARN TaskSetManager: Lost task 27.0 in stage 1.0 (TID 140): java.lang.OutOfMemoryError: GC overhead limit exceeded
26/11/18 03:52:30 ERROR TaskSetManager: Task 9 in stage 1.0 failed 4 times; aborting job
""")


def clickstream_sessionize(root: Path):
    """Runs fine, but 40 executors x 4 cores mostly wait on a few long tasks: paid-for idle cores."""
    log = EventLog("application_1700000000000_0063", "clickstream_sessionize",
                   {"spark.executor.memory": "16g", "spark.executor.cores": "4", "spark.executor.instances": "40",
                    "spark.sql.shuffle.partitions": "400", "spark.sql.adaptive.enabled": "true"},
                   executors=40, cores=4)
    log.plan("== Physical Plan ==\nAdaptiveSparkPlan\n+- Window [session_id]\n   +- Sort [user_id, ts]")
    scan_end = log.stage(0, "scan clickstream", 160, START + 7000, lambda k: random.randint(30000, 45000),
                         lambda k, d: {"Executor Run Time": d, "Executor CPU Time": d * 850_000,
                                       "JVM GC Time": int(d * 0.03), "Input Metrics": {"Bytes Read": 128 * MB},
                                       "Shuffle Write Metrics": {"Shuffle Bytes Written": 60 * MB}},
                         heap_bytes=int(4.1 * GB))
    end = log.stage(1, "sessionize window", 40, scan_end + 500, lambda k: random.randint(1_200_000, 1_500_000),
                    lambda k, d: {"Executor Run Time": d, "Executor CPU Time": d * 800_000,
                                  "JVM GC Time": int(d * 0.04), "Peak Execution Memory": 300 * MB,
                                  "Shuffle Read Metrics": {"Remote Bytes Read": 200 * MB, "Local Bytes Read": 40 * MB,
                                                           "Fetch Wait Time": 100}},
                    heap_bytes=int(4.1 * GB))
    log.finish(end)
    log.write(root / "application_1700000000000_0063", "26/11/18 04:00:01 INFO Executor: Running task\n")


def main(root: Path):
    random.seed(7)
    for build in (daily_sales_join, orders_enrichment, clickstream_sessionize):
        build(root)
    print(f"wrote 3 applications to {root}")


if __name__ == "__main__":
    main(Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).resolve().parent / "spark-events")
