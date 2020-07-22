package mapreduce.TFIDF;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TFIDFReducer extends Reducer<Text, Text, NullWritable, Text>{
    private MultipleOutputs<NullWritable, Text> mos = null;

    public void setup(Context context)throws IOException, InterruptedException{
        mos = new MultipleOutputs(context);
    }
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
        String filename = key.toString();
        for(Text tfidf:values)
        {
            mos.write(NullWritable.get(),tfidf,filename+"tfidf");
        }
    }
    public void cleanup(Context context) throws IOException, InterruptedException {
        if (mos != null) {
            mos.close();
            mos = null;
        }
    }
}
