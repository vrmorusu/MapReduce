import java.io.IOException;
import java.util.ArrayList;
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

// Count of all circles in flags
public class QJ_CircleCountInAllFlags extends Configured implements Tool{
	public static class CountryMapper extends Mapper <LongWritable, Text, Text, Text> {
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
			String[] data = line.split(",");

			context.write(new Text(data[0]), new Text(data[18]));
		}
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException{
			
		}
	}
	
	public static class CountryReducer extends Reducer <Text, Text, Text, Text> {
		int mCircleCount;
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{			
			for(Text s : values){
				if(Integer.valueOf(s.toString()) > 0){
					context.write(new Text(key), new Text(s));
					mCircleCount++;	
				}
			}
					
		}
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException{	
			context.write(new Text("Total Count of circles in all flags: "), new Text(String.valueOf(mCircleCount)));
		}
	}
	
	@Override
	public int run(String[] arg0) throws Exception {
		@SuppressWarnings("deprecation")
		Job job = new Job();
		
		job.setJarByClass(QJ_CircleCountInAllFlags.class);
		job.setJobName("Country Flag Project");
		job.setMapperClass(CountryMapper.class);
		job.setReducerClass(CountryReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));	
		
		return job.waitForCompletion(true)? 1:0;
	}
	
	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new QJ_CircleCountInAllFlags(), args);
		System.exit(exitCode);
	}


	
}
