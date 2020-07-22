package mapreduce.TFIDF;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

/**
 * @author zihan
 */
public class TFReducer extends Reducer<Text, DoubleWritable, NullWritable, Text> {
    private MultipleOutputs<NullWritable, Text> mos= null;
    @Override
    public void setup(Context context) throws IOException, InterruptedException{
        mos = new MultipleOutputs(context);
    }
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        String word = key.toString().split("#")[0];
        String filename = key.toString().split("#")[1];
        for(DoubleWritable tf:values)
        {
            Text temp = new Text(word+"\t"+tf.toString());
            mos.write(NullWritable.get(),temp,context.getProfileParams()+"#"+filename);
        }
    }
    public void cleanup(Context context) throws IOException, InterruptedException {
        if (mos != null) {
            mos.close();
            mos = null;
        }
    }
}
