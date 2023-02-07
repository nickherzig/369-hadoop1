package csc369;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.squareup.okhttp.Address;

public class QueryThree {
    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;
    public static final String ADDRESS_HARDCODED = "64.242.88.10";

    public static class MapperImpl extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
	protected void map(LongWritable key, Text value,
			   Context context) throws IOException, InterruptedException {
	    String[] sa = value.toString().split(" ");
	    Text address = new Text();
        IntWritable bytes = new IntWritable();
	    address.set(sa[0]);
        if (ADDRESS_HARDCODED.equals(address.toString())){
            bytes.set(Integer.parseInt(sa[9]));
            context.write(address, bytes);
        }
        }
    }

    public static class ReducerImpl extends Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable result = new IntWritable();
    
        @Override
	protected void reduce(Text responseCode, Iterable<IntWritable> intOne,
			      Context context) throws IOException, InterruptedException {
            int sum = 0;
            Iterator<IntWritable> itr = intOne.iterator();
        
            while (itr.hasNext()){
                sum  += itr.next().get();
            }
            result.set(sum);
            context.write(responseCode, result);
       }
    }
}
