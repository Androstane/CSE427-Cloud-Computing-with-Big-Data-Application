package example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class NameYearPartitioner<K2, V2> extends
		HashPartitioner<StringPairWritable, Text> {

	/**
	 * Partition Name/Year pairs according to the first string (last name) in the string pair so 
	 * that all keys with the same last name go to the same reducer, even if  second part
	 * of the key (birth year) is different.
	 */
	public int getPartition(StringPairWritable key, Text value, int numReduceTasks) {

		//get the first letter of the last name and conver it to ascii code
		int n = (int)key.getLeft().charAt(0);

        //put first letter start with letter with asc code smaller than 75 to the first reducer, otherwise put to second. 
		if (n>75){
			return 0;
		}
		else{
			return 1;
		}
		}
	
				
	}
