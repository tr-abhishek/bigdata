package insurance;
import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class mapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,IntWritable>{
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> values, Reporter r) throws IOException {
		
        	String[] str = value.toString().split(",");
		String county = str[2];
		values.collect(new Text(county), new IntWritable(1));
	}

}
