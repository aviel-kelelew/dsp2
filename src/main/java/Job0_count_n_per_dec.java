import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;


public class Job0_count_n_per_dec {
    private  static HashSet<String> stopWordsHashSet = new HashSet<>();

    public static class MapperClass extends Mapper<LongWritable, Text, Text, LongWritable>  {

        public void setup(Context context) throws IOException, InterruptedException{
            String wordStop=  context.getConfiguration().get("WordStopFile");
            try{
                for(String word : wordStop.split("\n")){
                    word = word.replace("\r","");
                    stopWordsHashSet.add(word);
                }
            } catch (Exception e){
                e.printStackTrace();
            }
        }

        public void map(LongWritable lineId, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            if(line.length == 5) {//need to be change to 5
                String wordToCheck = line[0].replace(" ","");
                boolean isContain = stopWordsHashSet.contains(wordToCheck.toLowerCase());
                if (!isContain && wordToCheck.length() >= 2) {
                    int year = Integer.parseInt(line[1])/10;
                    context.write(new Text(String.valueOf(year)),new LongWritable(Long.parseLong(line[2])));
                }
            }
            else { System.out.println("problem in the mapper of FirstJob - incorrect number of words");}
        }
    }

    public static class CombinerClass extends Reducer<Text,LongWritable,Text,LongWritable> {
        private long numberOfOcc;
        private String t;
        public void setup(Context context){
            numberOfOcc = 0;
            t="";
        }

        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
            System.out.println("combine :start the combine");
            for (LongWritable value : values) {
                if(!key.toString().equals(t)){
                    t = key.toString();
                    numberOfOcc = 0;
                }
                numberOfOcc = numberOfOcc + value.get();
            }
            context.write(key, new LongWritable(numberOfOcc));
        }
        public void cleanup(Context context)  {}
    }

    public static class ReducerClass extends Reducer<Text,LongWritable,Text,LongWritable> {
        private long numberOfOcc;
        private String t;

        public void setup(Context context){
            numberOfOcc = 0;
            t="";
        }
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
            System.out.println("combine :start the reduce class");
            for (LongWritable value : values) {
                if(!key.toString().equals(t)){
                    t = key.toString();
                    numberOfOcc = 0;
                }
                numberOfOcc = numberOfOcc + value.get();
            }
            System.out.println(numberOfOcc+" "+String.valueOf(numberOfOcc));
            context.write(key, new LongWritable(numberOfOcc));
        }
        public void cleanup(Context context)  { }
    }

    public static class PartitionerClass extends Partitioner<Text, LongWritable> {
        @Override
        public int getPartition(Text key, LongWritable text, int numPartitions) {
            return key.hashCode() % numPartitions;
        }
    }

    public static void CreateHashSetStopWords(MapperClass.Context context) throws FileNotFoundException {
        Configuration conf = context.getConfiguration();
        try{
            for(String word : conf.get("WordStopFile").split("\n")){
                word = word.replace("\r","");
                stopWordsHashSet.add(word);
            }
        } catch (Exception e){
            e.printStackTrace();
        }

    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        S3Client s3 = S3Client.builder().region(Region.US_EAST_1).build();
        software.amazon.awssdk.services.s3.model.GetObjectRequest request = software.amazon.awssdk.services.s3.model.GetObjectRequest.builder().key(args[1]).bucket("ass2bucket1").build();
        ResponseBytes<GetObjectResponse> responseBytes = s3.getObjectAsBytes(request);
        byte[] data = responseBytes.asByteArray();
        String stopWords = new String(data, StandardCharsets.UTF_8);
        Configuration conf0 = new Configuration();
        conf0.set("WordStopFile", stopWords);
        System.out.println("starting job 0");
        Job job0 = Job.getInstance(conf0,"preJob");
        job0.setJarByClass(Job0_count_n_per_dec.class);
        job0.setOutputKeyClass(Text.class);
        job0.setOutputValueClass(LongWritable.class);
        job0.setMapOutputKeyClass(Text.class);
        job0.setMapOutputValueClass(LongWritable.class);
        job0.setPartitionerClass(Job0_count_n_per_dec.PartitionerClass.class);
        job0.setMapperClass(Job0_count_n_per_dec.MapperClass.class);
        job0.setReducerClass(Job0_count_n_per_dec.ReducerClass.class);
        job0.setCombinerClass(Job0_count_n_per_dec.CombinerClass.class);
        FileInputFormat.addInputPath(job0, new Path(args[2])); // path need to be with one grams.
        FileOutputFormat.setOutputPath(job0, new Path("s3://ass2bucket1/output1"));
        job0.setInputFormatClass(SequenceFileInputFormat.class);
        job0.setOutputFormatClass(TextOutputFormat.class);
             job0.waitForCompletion(true);
             if (job0.isSuccessful()) {
                 System.out.println("Finish the pre job");
               } else {
               throw new RuntimeException("Job failed : " + job0);
              }

}
    }
