package com.info.mapreducetest;
//@see https://hadoop.apache.org/docs/r2.6.0/api/

/**
 * import all the required packages
 */
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

//Driver Class, main entry point of the prgram,
public class Driver {
    
    public static void main(String... args) throws IOException, InterruptedException, ClassNotFoundException{
        //check if the input file, and the output path directory is given as an input or not.
        if(args.length<2){
            System.out.println("Usage is [generic option] <input path> <output path>");
            System.exit(1);
        }
        //Create an instance of the JOB, needed to control the execution of our program.
        Job job=new Job();
        
        //Set the Jar by finding where a given class came from
        job.setJarByClass(Driver.class);
        //Set any user specified name for the Job
        job.setJobName("wordCount");
        
        //Process the input file (.txt,.csv) and to be used by the Mappers
        //Set the array of Paths as the list of inputs for the map-reduce job.
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        //Processed input to written toa  directory in HDFS, used by Reducers
        // Set the Path of the output directory for the map-reduce job.
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        //Name of the class which extends the Mapper Class or Name of the Mapper
        //which has the logic and performs Map-only JOB
        job.setMapperClass(MapperTest.class);
        //Name of the class which extends the reducer Class or Name of the Reducer
        //which has the logic and performs Reduce-only JOB
        job.setReducerClass(ReducerTest.class);
        
        //Result of the of Mapper and Reducer is a combination of (key, value) pairs.
        //set the output key datatype as Text for Reducer.
        job.setOutputKeyClass(Text.class);
        //set the output key datatype as Text for Reducer.
        //set the output key datatype for Reducer.
        job.setOutputValueClass(IntWritable.class);
        
        //Check the status of the JOB and then exit.
        System.exit(job.waitForCompletion(true)?0:1);
        
    }
}
