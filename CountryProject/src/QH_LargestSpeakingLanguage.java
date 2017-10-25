import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

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
public class QH_LargestSpeakingLanguage extends Configured implements Tool{
	public static class CountryMapper extends Mapper <LongWritable, Text, Text, LongWritable> {
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
			String[] data = line.split(",");
			int language = Integer.valueOf(data[5]);
			String languageKey = null;
			
			switch(language){
				case 1:
					languageKey = "English";
					break;
				case 2:
					languageKey = "Spanish";
					break;
				case 3:
					languageKey = "French";
					break;
				case 4:
					languageKey = "German";
					break;	
				case 5:
					languageKey = "Slavic";
					break;	
				case 6:
					languageKey = "Other Indo-European";
					break;
				case 7:
					languageKey = "Chinese";
					break;
				case 8:
					languageKey = "Arabic";
					break;
				case 9:
					languageKey = "Japanese";
					break;
				case 10:
					languageKey = "Other";
					break;
				default:
					languageKey = null;
					break;	
			}
			System.out.println(languageKey);
			context.write(new Text(languageKey), new LongWritable(1));
		}
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException{
			
		}
	}
	
	public static class CountryReducer extends Reducer <Text, LongWritable, Text, LongWritable> {
		HashMap<String, Integer> mlanguageMap ;
		
		@Override
		public void setup(Context context){
			mlanguageMap =   new HashMap<String, Integer>();
		}
		
		@Override
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException{
			int sum = 0;
			for (LongWritable value : values)
				sum += value.get();
			
			mlanguageMap.put(key.toString(), sum);	
		}
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException{
			Map.Entry<String, Integer> largest = null;
			for(Map.Entry<String,Integer> entry : mlanguageMap.entrySet()){
				context.write(new Text(entry.getKey()), new LongWritable(entry.getValue()));
				if((largest == null) || (entry.getValue() > largest.getValue()))
					largest = entry;
			}
			context.write(new Text("\nLargest speaking language: " + largest.getKey()), new LongWritable(largest.getValue()));
		}
	}
	
	
	
	@SuppressWarnings("deprecation")
	@Override
	public int run(String[] arg0) throws Exception {
		Job job = new Job();
		
		job.setJarByClass(QH_LargestSpeakingLanguage.class);
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
		int exitCode = ToolRunner.run(new QH_LargestSpeakingLanguage(), args);
		System.exit(exitCode);
	}


	
}
