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
public class QI_CommonColorCount extends Configured implements Tool{
    public static class CountryMapper extends Mapper <LongWritable, Text, Text, LongWritable> {
       
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String input = value.toString();
            String[] data = input.split(",");
            String[] color = {"red", "green", "blue", "gold", "white", "black", "orange"};
            
            for (int i=10, j=0; i<17; i++, j++){
            	System.out.println(data[i] + "\t" + i);
                if(data[i].equals("1")){
                	System.out.println("$ " + color[j]);
                    context.write(new Text(color[j]), new LongWritable(1));
                }
            }           
        }
       
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException{
           
        }
    }
   
    public static class CountryReducer extends Reducer <Text, LongWritable, Text, LongWritable> {
        HashMap<String, Integer> mColorCount = new HashMap<String, Integer>();
        
        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context){
        	int sum = 0;
        	for (LongWritable value : values)
				sum += value.get();
        	System.out.println(key.toString() + ":\t" + sum );
            mColorCount.put(key.toString(), sum);
        }
        
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException{
            Map.Entry<String, Integer> largest = null;
            for(Map.Entry<String, Integer> entry : mColorCount.entrySet()){
                context.write(new Text(entry.getKey()), new LongWritable(entry.getValue()));
                if((largest == null) || (entry.getValue() > largest.getValue())){
                    largest = entry;
                }
            }
           
            context.write(new Text("\n Largest used color: " + largest.getKey()), new LongWritable(largest.getValue()));
        }        
    }
   
    @SuppressWarnings("deprecation")
    @Override
    public int run(String[] arg0) throws Exception {
        Job job = new Job();
       
        job.setJarByClass(QF_LargestCountryInNEZone.class);
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

