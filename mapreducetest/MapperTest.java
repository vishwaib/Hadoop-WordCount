package com.info.mapreducetest;
//@see https://hadoop.apache.org/docs/r2.6.0/api/

/**
 * import all the required packages
 */
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//Custom class which extends a Mapper class as of Hadoop 2.x
//Define input and output (key, value) pairs of the Mapper class
/**
* Mapper<LongWritable,Text,Text,IntWritable>
* LongWritable input key address of the file record in memory
* Text input value
* Text output key
* IntWritable output value
*/
public class MapperTest extends Mapper<LongWritable,Text,Text,IntWritable>{
	IntWritable one=new IntWritable(1);
 	/**
 	*Override the map method of Mapper class
 	*Called once for each key/value pair in the input split. Most applications should override this, 
 	*but the default is the identity function.
 	*
 	* @param 
 	* LongWritable input key address of the file record in memory
 	* Text input value
 	* Context outputs as (key, value) as OPStream
 	*/
 	@Override
 	public void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException{
 		//split the input Text Object which holds the string on space
 		/**
 		 * Test = "how are you";
 		 * line = {"how", "are", "you"}
 		 */
 		String[] line=value.toString().split(" ");
 		//line is an String array of words in a sentence
 		for(String word:line){
 			//Output Map only Job (Text, IntWritable) type.
 			// writes ("how", 1), ("are", 1) ("you", 1)
 			context.write(new Text(word.toLowerCase()), one);
 		}

 	}
 }
