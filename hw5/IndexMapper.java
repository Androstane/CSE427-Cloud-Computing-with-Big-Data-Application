package stubs;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IndexMapper extends Mapper<Text, Text, Text, Text> {
  private Text output_word = new Text();
  private Text output_location = new Text();
  private String filename = "";
  @Override
  public void map(Text key, Text value, Context context) throws IOException,
      InterruptedException {

    /*
     * TODO implement
     */
    /*
     * read the input and split into words. 
     */
    String[] word = value.toString().split("\\w+");
    /*
     * loop through all words, convert all to lower case, write the word and its location (line). 
     * output in form (word, line)
     */
    for (String w:word){
    	w = w.toLowerCase();
    	output_word.set(w);
    	output_location.set(filename + "@" +key.toString());
    	context.write(output_word, output_location);
    	
    }
  }
  protected void setup(Context context) {
	  filename = ((FileSplit)context.getInputSplit()).getPath().getName();
  }
}