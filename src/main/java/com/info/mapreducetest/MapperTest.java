package com.info.mapreducetest;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperTest extends Mapper<LongWritable,Text,Text,IntWritable>{
    IntWritable one=new IntWritable(1);
    
    @Override
    public void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException{
        
        String[] line=value.toString().split(" ");
        for(String word:line){
            context.write(new Text(word.toLowerCase()), one);
        }
        
    }
}
