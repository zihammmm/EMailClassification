package mapreduce.TFIDF;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class EMailCombiner extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private DoubleWritable value = new DoubleWritable(0);
    private double wordnum = 0;

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
        if(key.toString().split("#")[0].equals("!")) {
            for(DoubleWritable num:values)
                wordnum = num.get() + wordnum;
        } else {
            int temp = 0;
            for(DoubleWritable num:values)
                temp += num.get();
            double tf = temp*1.0/wordnum;
            value.set(tf);
            context.write(key,value);
        }
    }
}
