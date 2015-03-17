SET default_parallel $reducers

--SET io.sort.mb 400
--SET io.sort.record.percent 0.15

--SET child.monitor.metrics.seconds 2
--SET child.monitor.counters true
--SET child.monitor.jvm.seconds 2
SET child.monitor.jstat.seconds 2

SET io.sort.mb $io_sort_mb
SET mapred.child.java.opts '$mapred_child_java_opts' 

SET mapred.job.reuse.jvm.num.tasks 1
SET job.name '$name $mapred_child_java_opts $io_sort_mb $reducers'
--SET job.name '$name $io_sort_mb $reducers'


--SET io.sort.record.percent 0.12
--SET mapred.inmem.merge.threshold 54
--SET mapred.reduce.parallel.copies 27
--SET mapred.job.shuffle.merge.percent 0.8
--SET mapred.job.shuffle.input.buffer.percent 0.8
--SET mapred.job.reduce.input.buffer.percent 1.0

--SET mapred.min.split.size 134217728

SET heapdump.map.input.records 4,469,603[10]
SET headdump.map.combine.input.records 1,806,750[10]

SET heapdump.task.attempt.ids attempt_201404111513_0003_m_000016_0




rmf output/pig_bench/html_join;
a = load '/data/rankings' using PigStorage('|') as (pagerank:int,pageurl,aveduration);
b = load '/syntheticData/brownTables/samples/sampleUservisits' using PigStorage('|') as (sourceIP,destURL,visitDate,adRevenue,userAgent,countryCode,languageCode,searchWord,duration);
d = JOIN a by pageurl, b by destURL using 'replicated';
store d into 'output/pig_bench/html_join';

