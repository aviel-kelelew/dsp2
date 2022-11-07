import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
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


import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;


public class Job1_calc_c_per_dec {
    private  static HashSet<String> stopWordsHashSet = new HashSet<>();
    private static HashMap<Integer,Long> hm = new HashMap<>();

    public static class MapperClass extends Mapper<LongWritable, Text, help3_KeyWordPerDecade, LongWritable>  {

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
            System.out.println("Start the FirstJob2 map");
            String[] line = value.toString().split("\t");
             if(line.length == 5) {
                String wordToCheck = line[0].replace(" ","");
                boolean isContain = stopWordsHashSet.contains(wordToCheck.toLowerCase());
                if (!isContain && wordToCheck.length()>= 2) {
                    int year = Integer.parseInt(line[1]);
                    context.write(new help3_KeyWordPerDecade(year,line[0],"*"),new LongWritable(Long.parseLong(line[2])) );
               }
            }
            else { System.out.println("problem in the mapper of FirstJob - incorrect number of words");}
        }
    }

    public static class CombinerClass extends Reducer<help3_KeyWordPerDecade,LongWritable, help3_KeyWordPerDecade,LongWritable> {
    private long numberOfOcc;
    private String t;

        public void setup(Context context){
            numberOfOcc = 0;
            t="";
        }
        public void reduce(help3_KeyWordPerDecade key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
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

    public static class ReducerClass extends Reducer<help3_KeyWordPerDecade,LongWritable, help3_KeyWordPerDecade, help2_ValueForFirstJob> {
        private String t;
        private long numberOfOcc;
        private HashMap<Integer,Long> hm;
        private int decade;

        public void setup(Context context) throws IOException, InterruptedException {
            numberOfOcc = 0;
            t = "";
            decade = 0;
            System.out.println("Start the reduce2 map");

                 String decadeTable = context.getConfiguration().get("decadeTable");
                 hm = PutThePreJobInHashMap(decadeTable);
            }
        public void reduce(help3_KeyWordPerDecade key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
            for (LongWritable value : values) {
                    if (!key.toString().equals(t)) {
                        t = key.toString();
                        numberOfOcc = 0;
                    }
                    numberOfOcc = numberOfOcc + value.get();
            }
                long N = 0;
            try {
                N = hm.get(key.getDecade());
            } catch (NullPointerException e) {
                System.out.println(e.getMessage());
            }
                System.out.println("the number is : " +N);
                context.write(key, new help2_ValueForFirstJob(N, numberOfOcc));
                System.out.println("finish the FirstJob2 reduce");
            }
        public void cleanup(Context context)  { }
    }

    public static class PartitionerClass extends Partitioner<help3_KeyWordPerDecade, LongWritable> {
        @Override
        public int getPartition(help3_KeyWordPerDecade key, LongWritable text, int numPartitions) {
            return key.hashCode() % numPartitions;
        }
    }

    public static HashMap<Integer, Long>  PutThePreJobInHashMap(String decadeTable) throws FileNotFoundException {
         HashMap<Integer, Long> ret = new HashMap<>();
         try{
             for(String decadeandCount : decadeTable.split("\n")){
                 decadeandCount = decadeandCount.replace("\r","");
                String[] decadeAndCount = decadeandCount.split("\t");
                 ret.put(Integer.parseInt(decadeAndCount[0]),Long.parseLong(decadeAndCount[1]));
             }
         } catch (Exception e){
             e.printStackTrace();
         }
         return ret;
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        S3Client s3 = S3Client.builder().region(Region.US_EAST_1).build();
        software.amazon.awssdk.services.s3.model.GetObjectRequest request = software.amazon.awssdk.services.s3.model.GetObjectRequest.builder().key(args[1]).bucket("ass2bucket1").build();
        ResponseBytes<GetObjectResponse> responseBytes = s3.getObjectAsBytes(request);
        byte[] data = responseBytes.asByteArray();
        String stopWords = new String(data, StandardCharsets.UTF_8);
        Configuration conf1 = new Configuration();
        conf1.set("WordStopFile", stopWords); // stopwords in args[0]
        String pathOutputPreJob = "";
        String decadeTable = "";
        for(int i=0; i<=13; i++){
            if(i<10){ pathOutputPreJob = "s3://ass2bucket1/output1/part-r-000"+""+"0"+i; }
            else{  pathOutputPreJob = "s3://ass2bucket1/output1/part-r-000"+""+i;  }
            Path path = new Path(pathOutputPreJob);
            FileSystem fs = path.getFileSystem(conf1);
            FSDataInputStream inputStream = fs.open(path);
            String tmpdacadeTable = IOUtils.toString(inputStream,"UTF-8");
            System.out.println(tmpdacadeTable);
            decadeTable = decadeTable+tmpdacadeTable;
            System.out.println("The decadeTable After adding:\n"+decadeTable);
            fs.close();
        }

        conf1.set("decadeTable",decadeTable);
        conf1.reloadConfiguration();
        System.out.println("starting job 1");
        Job job1 = Job.getInstance(conf1, "firstJob");
        job1.setJarByClass(Job1_calc_c_per_dec.class);
        job1.setOutputKeyClass(help3_KeyWordPerDecade.class);
        job1.setOutputValueClass(help2_ValueForFirstJob.class);
        job1.setMapOutputKeyClass(help3_KeyWordPerDecade.class);
        job1.setMapOutputValueClass(LongWritable.class);
        job1.setMapperClass(Job1_calc_c_per_dec.MapperClass.class);
        job1.setReducerClass(Job1_calc_c_per_dec.ReducerClass.class);
        job1.setCombinerClass(Job1_calc_c_per_dec.CombinerClass.class);
        job1.setPartitionerClass(Job1_calc_c_per_dec.PartitionerClass.class);
        FileInputFormat.addInputPath(job1, new Path(args[2]));
        FileOutputFormat.setOutputPath(job1, new Path("s3://ass2bucket1/output2"));  //the path from s3 need to be change"s3n://ass02/Step1"
        job1.setInputFormatClass(SequenceFileInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        job1.waitForCompletion(true);
        if (job1.isSuccessful()) {
            System.out.println("Finish the first job");
        } else {
                throw new RuntimeException("Job failed : " + job1);
        }

    }

}
