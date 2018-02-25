package stubs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCoMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
    /*
     * TODO implement
     */
	  /*
	   * Read the input and plit line to words. 
	   */
	  String line = value.toString();
	  String[] w = line.split("\\W+");
	  /*
	   * convert all to lower case 
	   */
	  for(int i=0;i<w.length-1;i++){
		  w[i] = w[i].toLowerCase();
		  w[i+1] = w[i+1].toLowerCase();
		  /*
		   * if both length>0, write the pair word1 , word2 
		   */
          if (w[i].length()>0){
              if (w[i+1].length()>0){
                  context.write(new Text(w[i]+", "+w[i+1]), new IntWritable(1));
        }
      }
	  }
		  
  }
}