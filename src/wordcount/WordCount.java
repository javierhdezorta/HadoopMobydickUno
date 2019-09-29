package wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class WordCount extends Configured implements Tool{
    public int run(String[] args)throws Exception{
        Configuration configuration=getConf();
        args=new GenericOptionsParser(configuration,args).getRemainingArgs();

        Job job=new Job(configuration,"wordcount");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(Mapping.class);
        job.setReducerClass(Reducing.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job,new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        int state = 0;
        if(job.waitForCompletion(true)){
            state=0;
        }
        return state;
    }


    public static void main(String[] args) throws Exception{
        int exitCode=ToolRunner.run(new WordCount(), args);
        System.exit(exitCode);
    }

}
