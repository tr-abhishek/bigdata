package word;  
  
import java.io.*;    
import java.util.*;    
import org.apache.hadoop.io.*;       
import org.apache.hadoop.mapred.*;    
    
public class mapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,IntWritable>
{       
    public void map(LongWritable key, Text value,OutputCollector<Text,IntWritable> output,     
        Reporter r) throws IOException
        {    
        
		String line = value.toString();    
		StringTokenizer st = new StringTokenizer(line); 
		String f;   
		while (st.hasMoreTokens())
		{    
		    f = st.nextToken();    
		    output.collect(new Text(f), new IntWritable(1));    
        	}    
    	}    
    
}  
