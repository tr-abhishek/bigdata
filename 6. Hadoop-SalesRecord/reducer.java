package salesrecord;

import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class reducer extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable> {

    public void reduce(Text key, Iterator<IntWritable> value1, OutputCollector<Text,IntWritable> values,Reporter r) throws IOException {
        String temp = key.toString();
        if(temp.substring(0, 9) == "_country_"){
            int total_sales = 0;
            while(value1.hasNext()){
                total_sales+=value1.next().get();
            }
            values.collect(key, new IntWritable(total_sales));
        } else{
            int payment_freq = 0;
            while(value1.hasNext()){
                payment_freq+=value1.next().get();
            }
            values.collect(key, new IntWritable(payment_freq));
        }
    }
}
