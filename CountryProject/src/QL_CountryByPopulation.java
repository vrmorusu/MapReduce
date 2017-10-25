import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class QL_CountryByPopulation extends Configured implements Tool{
	public static class CountryMapper extends Mapper <LongWritable, Text, Text, LongWritable> {
		
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
			String[] data = line.split(",");
			int pop = Integer.valueOf(data[4]);
			context.write(new Text(data[0]), new LongWritable(pop));
		}
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException{
			
		}
	}
	
	public static class CountryReducer extends Reducer <Text, LongWritable, Text, LongWritable> {
		TreeMap<Text, LongWritable> maxPopCountry = new TreeMap();
		
		@Override
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException{
			for(LongWritable value: values){
				maxPopCountry.put(new Text(key), new LongWritable(value.get()));
			}
			maxPopCountry.descendingMap().values();			
		}
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException{
			for(Map.Entry<Text,LongWritable> entry : maxPopCountry.entrySet())	
				context.write(new Text(entry.getKey()), new LongWritable(entry.getValue().get()));
		}
	}
	
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job();
		
		job.setJarByClass(QL_CountryByPopulation.class);
		job.setJobName("Country Flag Project");
		job.setMapperClass(CountryMapper.class);
		job.setReducerClass(CountryReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));	
		
		return job.waitForCompletion(true)? 1:0;
	}
	
	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new QL_CountryByPopulation(), args);
		System.exit(exitCode);
	}


	
}
