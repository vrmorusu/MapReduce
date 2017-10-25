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

// Counter count based on Landmass
public class QD_ColorCount extends Configured implements Tool{
	public static class CountryMapper extends Mapper <LongWritable, Text, Text, Text> {
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
			String[] data = line.split(",");
			ArrayList<String> color = new ArrayList<>();;
			
			color.add(data[28]);
			color.add(data[29]);
			//System.out.println(color.get(0) + "\t" + color.get(1));
			context.write(new Text(data[0]), new Text(color.get(0) + "\t" + color.get(1)));
		}
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException{
			
		}
	}
	
	public static class CountryReducer extends Reducer <Text, Text, Text, Text> {
		int mCount;
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			//String input = values.toString();
			
			for(Text s : values){
				String[] color = s.toString().split("\t");
				System.out.println(s );//color[0] + "\t" + color[1]);
				if(color[0].equals(color[1])){
					context.write(new Text(key), new Text(s));
					mCount++;
				}	
			}
					
		}
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException{	
			context.write(new Text("Total Count"), new Text(String.valueOf(mCount)));
		}
	}
	
	@Override
	public int run(String[] arg0) throws Exception {
		@SuppressWarnings("deprecation")
		Job job = new Job();
		
		job.setJarByClass(QD_ColorCount.class);
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
		int exitCode = ToolRunner.run(new QD_ColorCount(), args);
		System.exit(exitCode);
	}


	
}
