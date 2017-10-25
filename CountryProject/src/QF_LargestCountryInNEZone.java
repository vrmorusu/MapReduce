import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
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

// Counter count based on zone
public class QF_LargestCountryInNEZone extends Configured implements Tool{
	public static class CountryMapper extends Mapper <LongWritable, Text, Text, LongWritable> {
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
			String[] data = line.split(",");
			int landmassValue = Integer.valueOf(data[2]);
			String zoneKey = null;
			
			switch(landmassValue){
				case 1:
					zoneKey = "NE";
					context.write(new Text(data[0]), new LongWritable(Integer.valueOf(data[3])));
					break;
				case 2:
					zoneKey = "SE";
					break;
				case 3:
					zoneKey = "SW";
					break;
				case 4:
					zoneKey = "NW";
					break;	
				default:
					zoneKey = null;
					break;	
			}
			System.out.println(zoneKey);
			
		}
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException{
			
		}
	}
	
	public static class CountryReducer extends Reducer <Text, LongWritable, Text, LongWritable> {
		long largestPopValue = 0;
		String largestPopKey = null;
		@Override
		public void setup(Context context){
		}
		
		@Override
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException{

			for (LongWritable value : values){
				if(largestPopValue < value.get()){
					largestPopValue = value.get();
					largestPopKey = key.toString();
				}				
			}
			
		}
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException{
			context.write(new Text(largestPopKey), new LongWritable(largestPopValue));
		}
	}
	
	@SuppressWarnings("deprecation")
	@Override
	public int run(String[] arg0) throws Exception {
		Job job = new Job();
		
		job.setJarByClass(QI_CommonColorCount.class);
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
		int exitCode = ToolRunner.run(new QI_CommonColorCount(), args);
		System.exit(exitCode);
	}


	
}
