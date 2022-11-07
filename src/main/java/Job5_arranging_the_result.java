import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class Job5_arranging_the_result {

    public static class MapperClass extends Mapper<LongWritable, Text, help4_Probability, Text> {
        @Override

        public void map(LongWritable lineId, Text line, Context context) throws IOException, InterruptedException {
            String[] args = line.toString().split("\t");
            //<<decade,w1,w2,ratio><0>>
            double ratio = 0;
            if(!args[3].equals("NaN") && !args[3].equals("Infinity") ){ ratio =  Double.parseDouble(args[3]);}
            context.write(new help4_Probability(Integer.valueOf(args[2]),args[0],args[1],ratio),new Text(""));
        }
}

    public static class ReducerClass extends Reducer<help4_Probability, Text, Text, Text> {
        Integer count =100;
        public void reduce(help4_Probability key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text value: values){
             if(count > 0){
                   context.write(new Text(key.getDecade()+" "+key.getWord1()+" "+key.getWord2()),new Text(String.valueOf(key.getProbability())));
                   count --;
             }
             else{return;}
            }
        }
        public void cleanup(Context context) {
        }
    }

    public static class PartitionerClass extends Partitioner<help4_Probability, Text> {
        @Override
        public int getPartition(help4_Probability key, Text text, int numPartitions) {
            return key.ghashCode() % numPartitions;
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        System.out.println("starting job 5");
        Configuration conf5 = new Configuration();
        Job job5 = Job.getInstance(conf5,"last job");
        job5.setJarByClass(Job5_arranging_the_result.class);
        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(Text.class);
        job5.setMapOutputKeyClass(help4_Probability.class);
        job5.setMapOutputValueClass(Text.class);
        job5.setMapperClass(Job5_arranging_the_result.MapperClass.class);
        job5.setReducerClass(Job5_arranging_the_result.ReducerClass.class);
        job5.setPartitionerClass(Job5_arranging_the_result.PartitionerClass.class);
        job5.setNumReduceTasks(34);
        FileInputFormat.addInputPath(job5, new Path("s3://ass2bucket1/output5")); // path need to be with one grams.
        FileOutputFormat.setOutputPath(job5,new Path(args[4]));  //"s3://ass2bucket2/output5/" change
        job5.setInputFormatClass(TextInputFormat.class);
        job5.setOutputFormatClass(TextOutputFormat.class);
        if (job5.waitForCompletion(true)){
             System.out.println("finish all!");
         }
    }
}