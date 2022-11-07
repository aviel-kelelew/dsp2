import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Job3_join {

    public static class MapperClass extends Mapper<LongWritable, Text, help1_KeyForFirstJoin, Text> {
        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException, InterruptedException {
            String[] args = line.toString().split("\t");
            if (args[1].equals("*")) {   // we are in the case <<w1,*,decade><N,number of occ of w>>
                help1_KeyForFirstJoin tmp1=new help1_KeyForFirstJoin(args[2],args[0],"a");
                Text tm2 = new Text("from1gram" + "\t" + args[3] + "\t" + args[4]);
                context.write(tmp1, tm2);
            } else {  //we are in case <<w1,w2,decade><numberofoccw1w2>>. args[1] is for rememeber the second string.
                help1_KeyForFirstJoin tmp1=new help1_KeyForFirstJoin(args[2],args[0],"b");
                Text tm2 = new Text("from2gram" + "\t" + args[5] + "\t" + args[1]);
                context.write(tmp1,tm2);
            }
        }
    }

    public static class ReducerClass extends Reducer<help1_KeyForFirstJoin, Text, Text, Text> {
        String t;
        String numberofdecade;
        String numberOfOccW;
        String numberOfOcW1W2;
        String word1;
        String word2;
        String decade;

        public void setup(Context context) {
            t="";
            numberofdecade = "0";
            numberOfOccW = "0";
            numberOfOcW1W2 = "0";
            word1 = "";
            word2 = "";
            decade = "";

        }

        public void reduce(help1_KeyForFirstJoin key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
             for(Text value: values) {
                 String[] args = value.toString().split("\t");
                 if(args[0].equals("from1gram")){
                     numberofdecade = args[1];
                     numberOfOccW = args[2];
                 }
                 else if(args[0].equals("from2gram")){
                     String[] keyString = key.toString().split("\t");
                     word1 = keyString[1];
                     word2 = args[2];
                     decade = keyString[0];
                     numberOfOcW1W2 = args[1];
                     context.write(new Text(keyString[1]+"\t"+args[2]+"\t"+keyString[0]),new Text(numberOfOccW+"\t"+args[1]+"\t"+numberofdecade));
                 }
                 else{
                     System.out.println("There was a problem with the First Join - in Reduce");
                 }
             }
        }

        public void cleanup(Context context) {
        }
    }

    public static class PartitionerClass extends Partitioner<help1_KeyForFirstJoin, Text> {
        @Override
        public int getPartition(help1_KeyForFirstJoin key, Text text, int numPartitions) {
            return key.tmphashCode() % numPartitions;
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        System.out.println("starting job 3");
        Configuration conf3 = new Configuration();
        Job job3 = Job.getInstance(conf3,"ThirdJob");
        job3.setJarByClass(Job3_join.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        job3.setMapOutputKeyClass(help1_KeyForFirstJoin.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setMapperClass(Job3_join.MapperClass.class);
        job3.setReducerClass(Job3_join.ReducerClass.class);
        //job3.setCombinerClass(ThirdJob.CombinerClass.class);
        job3.setPartitionerClass(Job3_join.PartitionerClass.class);
        MultipleInputs.addInputPath(job3, new Path("s3://ass2bucket1/output3"),TextInputFormat.class); // path need to be with one grams.
        MultipleInputs.addInputPath(job3, new Path("s3://ass2bucket1/output2"),TextInputFormat.class);
        FileOutputFormat.setOutputPath(job3,new Path("s3://ass2bucket1/output4"));  //the path from s3 need to be change
        job3.setOutputFormatClass(TextOutputFormat.class);
        job3.waitForCompletion(true);
           if (job3.isSuccessful()){
            System.out.println("Finish the third job");
           }
          else {
             throw new RuntimeException("Job failed : " + job3);
           }

    }
}