package mapper;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {

	public static class TokenizerMapper extends
			Mapper<Object, Text, NullWritable, Text> {

		private final NullWritable one = NullWritable.get();
		private int max = 0;

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			int i = 0;
			
			while (itr.hasMoreTokens()) {
				String s = itr.nextToken();
				i++;
			}
			
			context.write(one, value);

			if (i > max)
				max = i;
		}

		public void cleanup(Context context) throws IOException,
				InterruptedException {
			System.err.println("[max] = " + max);
		}
	}

	// params: /LijieXu/Wikipedia/enwiki-20110405.txt /Output/Wikipedia
	public static void main(String[] args) throws Exception {

		int xmxArray[] = { 2000 };
		int xmsArray[] = { 0 };
		int ismbArray[] = { 400 };

		for (int xmxIndex = 0; xmxIndex < xmxArray.length; xmxIndex++) {
			for (int xmsIndex = 0; xmsIndex < xmsArray.length; xmsIndex++) {
				for (int ismbIndex = 0; ismbIndex < ismbArray.length; ismbIndex++) {

					int xmx = xmxArray[xmxIndex];
					int xms = xmsArray[xmsIndex] * xmx;
					int ismb = ismbArray[ismbIndex];

					Configuration conf = new Configuration();

					conf.setLong("mapred.min.split.size", 512 * 1024 * 1024L);
					conf.setLong("mapred.max.split.size", 512 * 1024 * 1024L);

					conf.setInt("mapred.reduce.tasks", 0);
					conf.setInt("io.sort.mb", ismb);

					if (xms == 0)
						conf.set("mapred.child.java.opts", "-Xmx" + xmx + "m");
					else
						conf.set("mapred.child.java.opts", "-Xmx" + xmx
								+ "m -Xms" + xms + "m");

					conf.setInt("child.monitor.jstat.seconds", 2);

					conf.set("fs.default.name", "hdfs://master:9000");
					conf.set("mapred.job.tracker", "master:9001");

					String[] otherArgs = new GenericOptionsParser(conf, args)
							.getRemainingArgs();
					if (otherArgs.length != 2) {
						System.err.println("Usage: WordCount <in> <out>");
						System.exit(2);
					}
					Job job = new Job(conf, "WordCount "
							+ conf.get("mapred.child.java.opts") + " ismb="
							+ ismb);
					job.setJarByClass(WordCount.class);
					job.setMapperClass(TokenizerMapper.class);

					job.setOutputKeyClass(NullWritable.class);
					job.setOutputValueClass(Text.class);
					FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
					FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

					FileSystem.get(conf).delete(new Path(otherArgs[1]), true);

					job.waitForCompletion(true);

					Thread.sleep(15000);
				}
			}
		}
	}

}
