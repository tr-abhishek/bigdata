package word;  
  
import java.io.*;    
import java.util.*;    
import org.apache.hadoop.io.*;       
import org.apache.hadoop.mapred.*;    
        
public class reducer  extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable> {    
    public void reduce(Text key, Iterator<IntWritable> value1,OutputCollector<Text,IntWritable> values,    
    	Reporter r) throws IOException {    
    	
    	int sum=0;    
    	while (value1.hasNext()) 
    	{    
    		sum+=value1.next().get();    
    	}    
    	values.collect(key, new IntWritable(sum));    
    }    
}  
