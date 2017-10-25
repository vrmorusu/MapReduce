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
public class QG_SmallestCountryInSALandmass extends Configured implements Tool{
	public static class CountryMapper extends Mapper <LongWritable, Text, Text, LongWritable> {
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
			String[] data = line.split(",");
			int landmassValue = Integer.valueOf(data[1]);
			String landmassKey = null;
			
			switch(landmassValue){
				case 1:
					landmassKey = "NAmerica";
					break;
				case 2:
					landmassKey = "SAmerica";
					context.write(new Text(data[0]), new LongWritable(Integer.valueOf(data[4])));
					break;
				case 3:
					landmassKey = "Europe";
					break;
				case 4:
					landmassKey = "Africa";
					break;	
				case 5:
					landmassKey = "Asia";
					break;	
				case 6:
					landmassKey = "Oceania";
					break;
				default:
					landmassKey = null;
					break;	
			}
			System.out.println(landmassKey);
		}
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException{
			
		}
	}
	
	public static class CountryReducer extends Reducer <Text, LongWritable, Text, LongWritable> {
		long smallestPopValue;
		String smallestPopKey = null;
		@Override
		public void setup(Context context){
		}
		
		@Override
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException{

			for (LongWritable value : values){
				if(!(value.get() > smallestPopValue)){
					smallestPopValue = value.get();
					smallestPopKey = key.toString();
					//mlandmassMap.put(key.toString(), pop);
				}				
			}
			
		}
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException{
			context.write(new Text(smallestPopKey), new LongWritable(smallestPopValue));
		}
	}
	
	@SuppressWarnings("deprecation")
	@Override
	public int run(String[] arg0) throws Exception {
		Job job = new Job();
		
		job.setJarByClass(QG_SmallestCountryInSALandmass.class);
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
		int exitCode = ToolRunner.run(new QG_SmallestCountryInSALandmass(), args);
		System.exit(exitCode);
	}


	
}
