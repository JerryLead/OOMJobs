package mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


// from http://puffsun.iteye.com/blog/1902837

public class WordCountOOM2 {

	public static class InputDocumentsTokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private Map<String, Integer> wordCounter;
		private final Text wordText = new Text();
		private final IntWritable totalCountInDocument = new IntWritable();

		private int inRec = 0;
		
		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);

			wordCounter = new HashMap<String, Integer>();
		}

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			inRec++;
			if(inRec < 9425)
				return;
			StringTokenizer st = new StringTokenizer(value.toString());
			
			// Count every word in a document
			while (st.hasMoreTokens()) {
				String word = st.nextToken();
				if (wordCounter.containsKey(word)) {
					wordCounter.put(word, wordCounter.get(word) + 1);
				} else {
					wordCounter.put(word, 1);
				}
			}
		}

		@Override
		public void cleanup(Context context) throws IOException,
				InterruptedException {

			// Emit each word as well as its count
			for (Map.Entry<String, Integer> entry : wordCounter.entrySet()) {
				wordText.set(entry.getKey());
				totalCountInDocument.set(entry.getValue());
				context.write(wordText, totalCountInDocument);
			}
			super.cleanup(context);
		}
	}

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	
	// /freeData/Wikipedia/txt/ /user/xulijie/output/WordCountOOM

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		conf.setInt("io.sort.mb", 400);
		//conf.set("mapred.child.java.opts", "-Xmx1000m");
		
		conf.setInt("child.monitor.jstat.seconds", 2);

		conf.set("fs.default.name", "hdfs://master:9000");
		conf.set("mapred.job.tracker", "master:9001");
		
		//conf.set("user.name", "xulijie");
		
		conf.set("mapred.child.java.opts",
				"-Xmx1000m -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps");
				
		conf.setInt("mapred.job.reuse.jvm.num.tasks", 1);
		
		// conf.setFloat("io.sort.record.percent", 0.2f);
		//conf.setFloat("io.sort.spill.percent", 0.95f);
		// conf.setFloat("mapred.job.shuffle.input.buffer.percent", 0.9f);
		// conf.setFloat("mapred.job.shuffle.merge.percent", 0.9f);
		//conf.setFloat("mapred.job.reduce.input.buffer.percent", 0.4f);
		//conf.set("mapred.job.tracker", "local");
		//conf.set("fs.default.name", "file:///");
		conf.setLong("mapred.min.split.size", 536870912);
		conf.setLong("mapred.max.split.size", 536870912);
		
		conf.setInt("mapred.map.max.attempts", 0);
		conf.setInt("mapred.reduce.max.attempts", 0);
		
		conf.set("heapdump.map.input.records", "9425;9426");
		conf.set("heapdump.task.attempt.ids", "attempt_201404152256_0087_m_000000_0");
		
		Job job = new Job(conf, "Word Count OOM");
		job.setJarByClass(WordCountOOM2.class);
		job.setMapperClass(InputDocumentsTokenizerMapper.class);
		//job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		// Delete the output directory if it exists already
		Path outputDir = new Path(otherArgs[1]);
		FileSystem.get(conf).delete(outputDir, true);
				
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}