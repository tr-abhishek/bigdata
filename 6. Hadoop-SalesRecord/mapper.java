package salesrecord;


import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>{
    public void map(LongWritable key, Text value, OutputCollector<Text,IntWritable> values,Reporter r) throws IOException {
        String[] line = value.toString().split(",");
        if(line[0].equals("Transaction_date")){
            return;
        }
        String country = "_country_" + line[7];
        String payment_type = "_payment_type_" + line[3];
        int price = Integer.parseInt(line[2]);
        values.collect(new Text(country), new IntWritable(price));
        values.collect(new Text(payment_type), new IntWritable(1));
    }
}
