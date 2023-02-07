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

public class QueryFour {
    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;
    public static final String URL_HARDCODED = "/twiki/bin/view/Main/WebHome";
    private static final IntWritable one = new IntWritable(1);

    public static class MapperImpl extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
	protected void map(LongWritable key, Text value,
			   Context context) throws IOException, InterruptedException {
	    String[] sa = value.toString().split(" ");
	    Text address = new Text();
        Text url = new Text();
        url.set(sa[6]);
        if (URL_HARDCODED.equals(url.toString())){
            address.set(sa[0]);
            context.write(address, one);
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
