package com.info.mapreducetest;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerTest extends Reducer<Text,IntWritable,Text,IntWritable>{
    
    @Override
    public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
        int count=0;
        for(IntWritable one:values){
            count=one.get()+count;
        }
        context.write(key, new IntWritable(count));
    }
    
}
