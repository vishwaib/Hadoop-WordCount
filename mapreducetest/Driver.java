package com.info.mapreducetest;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

public class Driver {
    
    public static void main(String... args) throws IOException, InterruptedException, ClassNotFoundException{
        if(args.length<2){
            System.out.println("Usage is [generic option] <input path> <output path>");
            System.exit(1);
        }
        Job job=new Job();
        
        job.setJarByClass(Driver.class);
        job.setJobName("wordCount");
        
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.setMapperClass(MapperTest.class);
        job.setReducerClass(ReducerTest.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        System.exit(job.waitForCompletion(true)?0:1);
        
    }
}
