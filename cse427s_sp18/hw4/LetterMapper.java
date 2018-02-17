
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LetterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	boolean caseSensitive = true;                                //set the default parameter to caseSensitve.
    @Override
    public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	    
    	String line = value.toString();
    	IntWritable length = new IntWritable();
    	/* 
    	 * Case Sensitive Part:
    	 */
	    if (caseSensitive == true) {                           
	    	for (String word : line.split("\\w+")){                  // split line to words
	  		  if (word.length()>0){
	  			  length.set(word.length());                         //get the length of the word
	  			  String letter = (word.substring(0,1));             //get the first letter of the word.
	  			  context.write(new Text(letter), length);           
	  			  }
	  		  }
	    	}
	    /*
	     * Case Insensitive Part:
	     */
		else {		  
			for (String word : line.split("\\w+")){                  // split line to words
		  		  if (word.length()>0){
		  			      length.set(word.length());                 //get the length of the word. 
		  				  String letter = (word.substring(0,1));     //split words to letters.
		  				  letter = letter.toLowerCase();             //convert all letters to lower case.
			              context.write(new Text(letter), length);
			  }
			  
		  }
		  
			  
		 
		}
    }
    /*
     * Using set up function to get the value of caseSensitive for each mapper.
     */
    public void setup(Context context){
    	Configuration conf = context.getConfiguration();
    	caseSensitive = conf.getBoolean("caseSensitive", true);        //default is true
    
    }
		  
	  }
  