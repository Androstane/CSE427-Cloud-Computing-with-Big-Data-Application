package stubs;

import java.util.HashSet;
import java.util.Set;
import java.io.FileReader;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.File;

public class SentimentPartitioner extends Partitioner<Text, IntWritable> implements
    Configurable {

  private Configuration configuration;
  Set<String> positive = new HashSet<String>();
  Set<String> negative = new HashSet<String>();


  @Override
  public void setConf(Configuration configuration) {
    /*
     * add positive-word txt and negative-word txt. 
     */
	  
	  File positiveword = new File("positive-words.txt");
	  File negativeword = new File("negative-words.txt");
	  /*
	   * add word in positive-word.txt file to hashset, ignore lines start with ";"
	   */
	  try{
		  BufferedReader positiveFile = new BufferedReader(new FileReader(positiveword));
		  String line = positiveFile.readLine();                                  //extract word by split lines
		  while (line != null){                                                   //if line is not empty 
			  if (line.charAt(0) != ';'){                                         //if the line is not started with ;, add it to positive. 
				  positive.add(line);
			  }
		  }
		  positiveFile.close();
	  }
      catch (FileNotFoundException e) {
    	// TODO Auto-generated catch block
	      e.printStackTrace();
      } catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	  /*
	   * add word in positive-word.txt file to hashset, ignore lines start with ";"
	   * The method is exactly same as used for positive-word.txt
	   */
	  try{
		  BufferedReader negativeFile = new BufferedReader(new FileReader(negativeword));
		  String line = negativeFile.readLine();
		  while (line != null){
			  if (line.charAt(0) != ';'){
				  negative.add(line);
			  }
		  }
		  negativeFile.close();
	  }
      catch (FileNotFoundException e) {
    	// TODO Auto-generated catch block
	      e.printStackTrace();
      } catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
  }


  /**
   * Implement the getConf method for the Configurable interface.
   */
  @Override
  public Configuration getConf() {
    return configuration;
  }

  /**
   * You need to implement the getPartition method for a partitioner class.
   * This method receives the words as keys (i.e., the output key from the mapper.)
   * It should return an integer representation of the sentiment category
   * (positive, negative, neutral).
   * 
   * For this partitioner to work, the job configuration must have been
   * set so that there are exactly 3 reducers.
   */
  public int getPartition(Text key, IntWritable value, int numReduceTasks) {
 
	  if (positive.contains(key.toString())){                  //return 0 if it contains positive key
		  return 0;
	  }
	  if (negative.contains(key.toString())){                  //return 1 if it contains negative key but not positive key
		  return 1;
	  }
	  else{
		  return 2;                                            //return 2 if it contains neither positive nor negative key 
	  }
	  
	  
  }
}
