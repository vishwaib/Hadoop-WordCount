package com.info.mapreducetest;
//@see https://hadoop.apache.org/docs/r2.6.0/api/

/**
 * import all the required packages
 */
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//Custom class which extends a Reducer class as of Hadoop 2.x
//Define input and output (key, value) pairs of the Reducer class
/**
* Reducer<Text,IntWritable,Text,IntWritable>
* Text input vkey
* IntWritable input value
* Text output key
* IntWritable output value
*/
public class ReducerTest extends Reducer<Text,IntWritable,Text,IntWritable>{
    /**
 	*Override the reduce method of Reducer class
 	*This method is called once for each key. Most applications should override this,  	
 	*
 	* @param 
 	* Text input value is grouped on similar keys
 	* Iterable<IntWritable> array of IntWritable 
 	* Context outputs as (key, value) as OPStream
 	*/
    @Override
    public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
        int count=0;
        // values ("how", 1) as Text key is grouped on similar keys, we have values as an IntWritable array
        for(IntWritable one:values){
            count=one.get()+count;
        }
        // writes ("how", 1)
        context.write(key, new IntWritable(count));
    }
    
}
