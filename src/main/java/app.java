import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;


public class app {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        try {


            /**------------------------jpb0-------------------------- **/


            S3Client s3 = S3Client.builder().region(Region.US_EAST_1).build();
            software.amazon.awssdk.services.s3.model.GetObjectRequest request = software.amazon.awssdk.services.s3.model.GetObjectRequest.builder().key(args[1]).bucket("ass2bucket2").build();
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
            FileOutputFormat.setOutputPath(job0, new Path("s3://ass2bucket2/output1"));
            job0.setInputFormatClass(SequenceFileInputFormat.class);
            job0.setOutputFormatClass(TextOutputFormat.class);
             job0.waitForCompletion(true);
             if (job0.isSuccessful()) {
                 System.out.println("Finish the pre job");
               } else {
               throw new RuntimeException("Job failed : " + job0);
              }


            /**------------------------jpb1-------------------------- **/


            Configuration conf1 = new Configuration();
            conf1.set("WordStopFile", stopWords); // stopwords in args[0]
            String pathOutputPreJob = "";
            String decadeTable = "";
            for(int i=0; i<=26; i++){
                if(i<10){ pathOutputPreJob = "s3://ass2bucket2/output1/part-r-0000"+""+"0"+i; }
                else{  pathOutputPreJob = "s3://ass2bucket2/output1/part-r-0000"+""+i;  }
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
            job1.setInputFormatClass(TextInputFormat.class);
            job1.setInputFormatClass(SequenceFileInputFormat.class);
            MultipleInputs.addInputPath(job1, new Path(args[2]),SequenceFileInputFormat.class);
            FileInputFormat.addInputPath(job1, new Path(args[2]));
            FileOutputFormat.setOutputPath(job1, new Path("s3://ass2bucket2/output2"));  //the path from s3 need to be change"s3n://ass02/Step1"
            job1.setInputFormatClass(SequenceFileInputFormat.class);
            job1.setOutputFormatClass(TextOutputFormat.class);
             job1.waitForCompletion(true);
             if (job1.isSuccessful()) {
                System.out.println("Finish the first job");
              } else {
                throw new RuntimeException("Job failed : " + job1);
              }


           /**------------------------jpb2-------------------------- **/


           Configuration conf2 = new Configuration();
           conf2.set("WordStopFile", stopWords); // stopwords in args[0]
            System.out.println("starting job 2");
            Job job2 = Job.getInstance(conf2,"SecondJob");
            job2.setJarByClass(Job2_calc_c1_c2_per_dec.class);
            job2.setOutputKeyClass(help3_KeyWordPerDecade.class);
            job2.setOutputValueClass(help2_ValueForFirstJob.class);
            job2.setMapOutputKeyClass(help3_KeyWordPerDecade.class);
            job2.setMapOutputValueClass(LongWritable.class);
            job2.setMapperClass(Job2_calc_c1_c2_per_dec.MapperClass.class);
            job2.setReducerClass(Job2_calc_c1_c2_per_dec.ReducerClass.class);
            job2.setCombinerClass(Job2_calc_c1_c2_per_dec.CombinerClass.class);
            job2.setPartitionerClass(Job2_calc_c1_c2_per_dec.PartitionerClass.class);
            SequenceFileInputFormat.addInputPath(job2, new Path(args[3])); // path need to be with one grams.
            FileOutputFormat.setOutputPath(job2,new Path("s3://ass2bucket2/output3"));  //the path from s3 need to be change
            job2.setInputFormatClass(SequenceFileInputFormat.class);
            job2.setOutputFormatClass(TextOutputFormat.class);
              job2.waitForCompletion(true);
              if (job2.isSuccessful()){
                  System.out.println("Finish the second job");
              }
             else {
               throw new RuntimeException("Job failed : " + job2);
             }

            /**------------------------jpb3 -------------------------- **/


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
            job3.setPartitionerClass(Job3_join.PartitionerClass.class);
            MultipleInputs.addInputPath(job3, new Path("s3://ass2bucket2/output3"),TextInputFormat.class); // path need to be with one grams.
            MultipleInputs.addInputPath(job3, new Path("s3://ass2bucket2/output2"),TextInputFormat.class);
            FileOutputFormat.setOutputPath(job3,new Path("s3://ass2bucket2/output4"));  //the path from s3 need to be change
            job3.setOutputFormatClass(TextOutputFormat.class);
             job3.waitForCompletion(true);
               if (job3.isSuccessful()){
                System.out.println("Finish the third job");
             }
             else {
                   throw new RuntimeException("Job failed : " + job3);
               }

            /**------------------------jpb4 -------------------------- **/


            System.out.println("starting job 4");
            Configuration conf4 = new Configuration();
            Job job4 = Job.getInstance(conf4,"second join");
            job4.setJarByClass(Job4_join_all_details.class);
            job4.setOutputKeyClass(Text.class);
            job4.setOutputValueClass(Text.class);
            job4.setMapOutputKeyClass(help1_KeyForFirstJoin.class);
            job4.setMapOutputValueClass(Text.class);
            job4.setMapperClass(Job4_join_all_details.MapperClass.class);
            job4.setReducerClass(Job4_join_all_details.ReducerClass.class);
            job4.setPartitionerClass(Job4_join_all_details.PartitionerClass.class);
              job3.setNumReduceTasks(32);
            job4.setInputFormatClass(TextInputFormat.class);
            MultipleInputs.addInputPath(job4, new Path("s3://ass2bucket2/output2"),TextInputFormat.class); // path need to be with one grams.
            MultipleInputs.addInputPath(job4, new Path("s3://ass2bucket2/output4"),TextInputFormat.class);
            FileOutputFormat.setOutputPath(job4,new Path("s3://ass2bucket2/output5"));  //"s3://ass2bucket2/output5/" change
            job4.setOutputFormatClass(TextOutputFormat.class);
            job4.waitForCompletion(true);
              if (job4.isSuccessful()){
                System.out.println("Finish the second job");
               }
               else {
              throw new RuntimeException("Job failed : " + job4);
             }

            /**------------------------jpb5 -------------------------- **/


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
            job5.setNumReduceTasks(32);
            FileInputFormat.addInputPath(job5, new Path("s3://ass2bucket2/output5")); // path need to be with one grams.
            FileOutputFormat.setOutputPath(job5,new Path(args[4]));  //"s3://ass2bucket2/output5/" change
            job5.setInputFormatClass(TextInputFormat.class);
            job5.setOutputFormatClass(TextOutputFormat.class);
            if (job5.waitForCompletion(true)){
                System.out.println("finish all!");
            }



            /**------------------------Job controller -------------------------- **/


            ControlledJob jobZeroControl = new ControlledJob(job0.getConfiguration());
            jobZeroControl.setJob(job0);
            ControlledJob jobOneControl = new ControlledJob(job1.getConfiguration());
            jobOneControl.setJob(job1);
            ControlledJob jobTwoControl = new ControlledJob(job2.getConfiguration());
            jobTwoControl.setJob(job2);
            ControlledJob jobThreeControl = new ControlledJob(job3.getConfiguration());
            jobThreeControl.setJob(job3);
            ControlledJob jobFourControl = new ControlledJob(job4.getConfiguration());
            jobFourControl.setJob(job4);
            ControlledJob jobFiveControl = new ControlledJob(job5.getConfiguration());
            jobFiveControl.setJob(job5);
            JobControl jobControl = new JobControl("job-control");
            jobControl.addJob(jobZeroControl);
            jobControl.addJob(jobOneControl);
            jobControl.addJob(jobThreeControl);
            jobControl.addJob(jobTwoControl);
            jobControl.addJob(jobFourControl);
            jobControl.addJob(jobFiveControl);
            jobOneControl.addDependingJob(jobZeroControl); // this condition makes the job-2 wait until job-1 is done
            jobThreeControl.addDependingJob(jobOneControl);
            jobThreeControl.addDependingJob(jobTwoControl);
            jobFourControl.addDependingJob(jobThreeControl);
            jobFiveControl.addDependingJob(jobFourControl);
            Thread jobControlThread = new Thread(jobControl);
            jobControlThread.start();
            int code = 0;
            while (!jobControl.allFinished()) {
                code = jobControl.getFailedJobList().size() == 0 ? 0 : 1;
                Thread.sleep(1000);
            }
            for(ControlledJob j : jobControl.getFailedJobList())
                System.out.println(j.getMessage());
            System.exit(code);
        }  catch (IOException | InterruptedException  e) {
            e.printStackTrace();
        }

    }
}
