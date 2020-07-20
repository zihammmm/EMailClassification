package mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class EMailCombiner extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private DoubleWritable value = new DoubleWritable(0);
    private double wordnum = 0;

    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
        if(key.toString().split("#")[0].equals('!'))
        {
            for(IntWritable num:values)
                wordnum += num.get();
        }
        else
        {
            int temp = 0;
            for(IntWritable num:values)
                temp += num.get();
            double tf = temp*1.0/wordnum;
            value.set(tf);
            context.write(key,value);
        }
    }
}
