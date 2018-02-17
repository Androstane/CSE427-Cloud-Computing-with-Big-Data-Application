package stubs;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.junit.Test;

public class SentimentPartitionTest {

	SentimentPartitioner spart;

	@Test
	public void testSentimentPartition() {

		spart=new SentimentPartitioner<Text, IntWritable>();
		spart.setConf(new Configuration());
		int result;		
		
		/*
		 * A test for word "beauty" with expected outcome 0 would   
		 * look like this:
		 */
		result = spart.getPartition(new Text("beauty"), new IntWritable(23), 3);
		assertEquals(result,0);	

		/*
		 * Test the words "love", "deadly", and "zodiac". 
		 * The expected outcomes should be 0, 1, and 2. 
		 */
       
 		/*
		 * Compare result of love with 0.
		 */      
		 int result_love = spart.getPartition(new Text("love"), new IntWritable(), 3);
	     assertEquals(result_love,0);
	     /*
	      * Compare result of deadly with 1.
	      */

	     int result_deadly = spart.getPartition(new Text("deadly"), new IntWritable(), 3);
	     assertEquals(result_deadly,1);
	    
	     /*Compare result of zodiac with 2.
	      *  
	      */
	     int result_zodiac = spart.getPartition(new Text("zodiac"), new IntWritable(), 3);
	     assertEquals(result_zodiac,2);
		
	}

}
