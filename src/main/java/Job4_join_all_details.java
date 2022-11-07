import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

//<w1w2Decade,value>
//<w1decase, value>
public class Job4_join_all_details {

    public static class MapperClass extends Mapper<LongWritable, Text, help1_KeyForFirstJoin, Text> {
        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException, InterruptedException {
            String[] args = line.toString().split("\t");
            if (args[1].equals("*")) {   // we are in the case <<w1,*,decade><N,number of occ of w>>
                help1_KeyForFirstJoin tmp1=new help1_KeyForFirstJoin(args[2],args[0],"a");
                Text tm2 = new Text("from1gram" + "\t" + args[4]);
                context.write(tmp1, tm2);
              //  System.out.println("the key is: "+tmp1+" the value is: "+tm2);
            } else {  //we are in case <<w1,w2,decade><W1,C1,C12=numberofoccw1w2,N>>. now we are going on W2
                context.write( new help1_KeyForFirstJoin(args[2],args[1],"b"), new Text("from2gram" + "\t" + args[0]+"\t"+ args[3]+"\t"+args[4]+"\t"+args[5]));
           /////                                   <decade><word2>                                                 <word1>       <C1>          <c12>       <N>
            }
        }
    }

    public static class ReducerClass extends Reducer<help1_KeyForFirstJoin, Text, Text, Text> {
        String t;
        String numberOfOccW2;

        public void setup(Context context) {
            t="";
            numberOfOccW2 = "0";
        }

        public void reduce(help1_KeyForFirstJoin key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text value: values) {
                String[] args = value.toString().split("\t");
                System.out.println(args[0]);
                if(args[0].equals("from1gram")){
                    numberOfOccW2 = args[1];
                }
                else if(args[0].equals("from2gram")){
                    String[] keyString = key.toString().split("\t");
                  //  <w1,w2,decade>
                    double c1 = Double.parseDouble(args[2]);
                    double c2 = Double.parseDouble(numberOfOccW2); /////tocheck
                    double c12 = Double.parseDouble(args[3]);
                    double N = Double.parseDouble(args[4]);
                    System.out.println(key.toString());
                  //  System.out.println("Before using p :\n"+"c1 is:"+c1+", c2 is:"+c2+", c12 is:"+c12+", N is:"+N);
                    double p =0.5; ///todelete
                    if(N!=0){p = c2/N ;}
                    double p1 = 0.25;///todelete
                    if(c1!=0){p1 =  c12/c1;}
                    double p2 = (c2 -c12)/2;
                    if(N-c1 !=0){p2 = (c2 -c12)/ (N -c1);} ///to deleten
                    //System.out.println("after using:"+"c1 is:"+c1+" c2 is:"+c2+" c12 is:"+c12+" N is:"+N+" p is:"+p+" p1 is:"+p1+" p2 is:"+p2);
                    double cal = (Math.log(LFunction(c12, c1, p)) + Math.log(LFunction(c2 - c12,N - c1,p)) - Math.log(LFunction(c12, c1, p1)) - Math.log(LFunction(c2 - c12,N - c1,p2)))*(-2.0);
                    //System.out.println("Before enter to context: "+ "ratio: "+cal);
                    context.write(new Text(args[1]+"\t"+keyString[1]+"\t"+keyString[0]+"\t"+cal), new Text(""));
                    //context.write(new Probability(Integer.parseInt(keyString[0]),args[1],args[1],cal), new IntWritable((0)));
                }
                else{
                    System.out.println("There was a problem with the First Join - in Reduce");
                }
            }
        }

        public void cleanup(Context context) {
        }

    }public static double LFunction(double k , double n, double x){
        //System.out.println("In the LFunction:"+Math.pow(x, k) * Math.pow((1 - x) , (n -k ))+"and after log:"+Math.log(Math.pow(x, k) * Math.pow((1 - x) , (n -k ))));
        return  Math.pow(x, k) * Math.pow((1.0 - x) , (n -k )) ;
    }

    public static class PartitionerClass extends Partitioner<help1_KeyForFirstJoin, Text> {
        @Override
        public int getPartition(help1_KeyForFirstJoin key, Text text, int numPartitions) {
            return key.tmphashCode() % numPartitions;
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
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
        //job3.setCombinerClass(ThirdJob.CombinerClass.class);
        job4.setPartitionerClass(Job4_join_all_details.PartitionerClass.class);
        //  job3.setNumReduceTasks(32);

        //job4.setInputFormatClass(TextInputFormat.class);


        MultipleInputs.addInputPath(job4, new Path("s3://ass2bucket1/output2"),TextInputFormat.class); // path need to be with one grams.
        MultipleInputs.addInputPath(job4, new Path("s3://ass2bucket1/output4"),TextInputFormat.class);
        FileOutputFormat.setOutputPath(job4,new Path("s3://ass2bucket1/output5"));  //"s3://ass2bucket2/output5/" change
         job4.setOutputFormatClass(TextOutputFormat.class);
        job4.waitForCompletion(true);
          if (job4.isSuccessful()){
             System.out.println("Finish the second job");
            }
          else {
          throw new RuntimeException("Job failed : " + job4);
         }
    }
}