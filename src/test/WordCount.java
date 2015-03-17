/**
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package test;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		int t = 0;

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);

//			if (++t == 10) {
//				System.err.println("Counters:");
//				System.err.println("REDUCE_STARTINPUT_RECORDS = " + 
//						context.getCounter(Task.Counter.REDUCE_STARTINPUT_RECORDS).getValue());
//				
//				System.err.println("REDUCE_INPUT_RECORDS = " +
//						context.getCounter(Task.Counter.REDUCE_INPUT_RECORDS).getValue());
//				
//			}


			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}

		conf.setInt("io.sort.mb", 400);
		// conf.set("mapred.child.java.opts", "-Xmx1000m");

		conf.setInt("child.monitor.jstat.seconds", 2);

		conf.set("fs.default.name", "hdfs://master:9000");
		conf.set("mapred.job.tracker", "master:9001");

		// conf.set("user.name", "xulijie");

		conf.set("mapred.child.java.opts",
				"-Xmx1000m -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps");

		conf.setInt("mapred.job.reuse.jvm.num.tasks", 1);

		// conf.setFloat("io.sort.record.percent", 0.2f);
		// conf.setFloat("io.sort.spill.percent", 0.95f);
		// conf.setFloat("mapred.job.shuffle.input.buffer.percent", 0.9f);
		// conf.setFloat("mapred.job.shuffle.merge.percent", 0.9f);
		// conf.setFloat("mapred.job.reduce.input.buffer.percent", 0.4f);
		// conf.set("mapred.job.tracker", "local");
		// conf.set("fs.default.name", "file:///");

		conf.setLong("mapred.min.split.size", 64 * 1024 * 1024L);
		conf.setLong("mapred.max.split.size", 64 * 1024 * 1024L);
		
		conf.setInt("mapred.map.max.attempts", 0);
		conf.setInt("mapred.reduce.max.attempts", 0);

		Job job = new Job(conf, "word count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		//job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		FileSystem.get(conf).delete(new Path(otherArgs[1]), true);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
