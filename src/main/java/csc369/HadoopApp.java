package csc369;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class HadoopApp {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Hadoop example");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

	if (otherArgs.length < 3) {
	    System.out.println("Expected parameters: <job class> <input dir> <output dir>");
	    System.exit(-1);
	} else if ("WordCount".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(WordCount.ReducerImpl.class);
	    job.setMapperClass(WordCount.MapperImpl.class);
	    job.setOutputKeyClass(WordCount.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(WordCount.OUTPUT_VALUE_CLASS);
	} else if ("AccessLog".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(AccessLog.ReducerImpl.class);
	    job.setMapperClass(AccessLog.MapperImpl.class);
	    job.setOutputKeyClass(AccessLog.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(AccessLog.OUTPUT_VALUE_CLASS);
    } else if ("AccessLog2".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(AccessLog2.ReducerImpl.class);
	    job.setMapperClass(AccessLog2.MapperImpl.class);
	    job.setOutputKeyClass(AccessLog2.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(AccessLog2.OUTPUT_VALUE_CLASS);
	} else if ("QueryOne".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(QueryOne.ReducerImpl.class);
	    job.setMapperClass(QueryOne.MapperImpl.class);
	    job.setOutputKeyClass(QueryOne.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(QueryOne.OUTPUT_VALUE_CLASS);
	} else if ("QueryTwo".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(QueryTwo.ReducerImpl.class);
	    job.setMapperClass(QueryTwo.MapperImpl.class);
	    job.setOutputKeyClass(QueryTwo.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(QueryTwo.OUTPUT_VALUE_CLASS);
	} else if ("QueryThree".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(QueryThree.ReducerImpl.class);
	    job.setMapperClass(QueryThree.MapperImpl.class);
	    job.setOutputKeyClass(QueryThree.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(QueryThree.OUTPUT_VALUE_CLASS);
	} else if ("QueryFour".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(QueryFour.ReducerImpl.class);
	    job.setMapperClass(QueryFour.MapperImpl.class);
	    job.setOutputKeyClass(QueryFour.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(QueryFour.OUTPUT_VALUE_CLASS);
	} else if ("QueryFive".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(QueryFive.ReducerImpl.class);
	    job.setMapperClass(QueryFive.MapperImpl.class);
	    job.setOutputKeyClass(QueryFive.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(QueryFive.OUTPUT_VALUE_CLASS);
	} else if ("QuerySix".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(QuerySix.ReducerImpl.class);
	    job.setMapperClass(QuerySix.MapperImpl.class);
	    job.setOutputKeyClass(QuerySix.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(QuerySix.OUTPUT_VALUE_CLASS);
	} else {
	    System.out.println("Unrecognized job: " + otherArgs[0]);
	    System.exit(-1);
	}

        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        System.exit(job.waitForCompletion(true) ? 0: 1);
    }

}
