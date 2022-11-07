
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import java.io.IOException;


public class test {
    public static void main(String[] args) throws IOException {
        AWSCredentialsProvider credentials =new EnvironmentVariableCredentialsProvider();
        AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder.standard()
                .withCredentials(credentials)
                .withRegion("us-east-1")
                .build();


        HadoopJarStepConfig step1_config = new HadoopJarStepConfig()
                .withJar("s3://ass2bucket2/PreJob.jar")
                .withMainClass("CountNPerDecade")
                .withArgs("s3://ass2bucket2","heb-stopwords.txt","s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data","s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data","s3://ass2bucket2/output7");

               StepConfig step1 = new StepConfig()
                .withName("Step1")
                .withHadoopJarStep(step1_config)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        HadoopJarStepConfig step2_config = new HadoopJarStepConfig()
                .withJar("s3://ass2bucket2/FirstJob.jar")
                .withMainClass("FirstJob")
                .withArgs("s3://ass2bucket2","heb-stopwords.txt","s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data","s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data","s3://ass2bucket2/output8");

        StepConfig step2 = new StepConfig()
                .withName("Step2")
                .withHadoopJarStep(step2_config)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        HadoopJarStepConfig step3_config = new HadoopJarStepConfig()
          .withJar("s3://ass2bucket2/SecondJob.jar")
          .withMainClass("SecondJob")
          .withArgs("s3://ass2bucket2","heb-stopwords.txt","s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data","s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data","s3://ass2bucket2/output9");

        StepConfig step3 = new StepConfig()
                .withName("Step3")
                .withHadoopJarStep(step3_config)
                .withActionOnFailure("TERMINATE_JOB_FLOW");


        HadoopJarStepConfig step4_config = new HadoopJarStepConfig()
                .withJar("s3://ass2bucket2/ThirdJob.jar")
                .withMainClass("ThirdJob")
                .withArgs("s3://ass2bucket2","heb-stopwords.txt","s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data","s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data","s3://ass2bucket2/output10");
        System.out.println("hstep1 args= "+step4_config.getArgs().toString());

        StepConfig step4 = new StepConfig()
                .withName("Step4")
                .withHadoopJarStep(step4_config)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        HadoopJarStepConfig step5_config = new HadoopJarStepConfig()
                .withJar("s3://ass2bucket2/JoinAllDetails.jar")
                .withMainClass("JoinAllDetails")
                .withArgs("s3://ass2bucket2","heb-stopwords.txt","s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data","s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data","s3://ass2bucket2/output11");
        System.out.println("hstep1 args= "+step5_config.getArgs().toString());

        StepConfig step5 = new StepConfig()
                .withName("Step5")
                .withHadoopJarStep(step5_config)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        HadoopJarStepConfig step6_config = new HadoopJarStepConfig()
                .withJar("s3://ass2bucket2/ArrangingTheResult.jar")
                .withMainClass("ArrangingTheResult")
                .withArgs("s3://ass2bucket2","heb-stopwords.txt","s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data","s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data","s3://ass2bucket2/output6");
        System.out.println("hstep1 args= "+step6_config.getArgs().toString());

        StepConfig step6 = new StepConfig()
                .withName("Step6")
                .withHadoopJarStep(step6_config)
                .withActionOnFailure("TERMINATE_JOB_FLOW");


        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(8)
                .withMasterInstanceType(InstanceType.M4Large.toString())
                .withSlaveInstanceType(InstanceType.M4Large.toString())
                .withHadoopVersion("2.6.0").withEc2KeyName("vockey")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("ASS2-with - stackoverflow- with stop word condition")
                .withInstances(instances)
                .withSteps(step3,step4,step5,step6)
                .withLogUri("s3://ass2bucket2/logs/")
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withReleaseLabel("emr-5.11.0");

        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();

        System.out.println("Ran job flow with id: " + jobFlowId);
    }
}
