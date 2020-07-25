package TFIDF;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.DoubleWritable;

import java.io.IOException;

public class IDFReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
    private int filenum = 0;
    public void setup(Context context)throws IOException, InterruptedException{
        filenum = Integer.parseInt(context.getProfileParams());
        System.err.println(filenum);
    }
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)throws IOException, InterruptedException{
        double wordnum = 0;
        for(DoubleWritable num: values)
            wordnum = wordnum + num.get();
        Text newkey = new Text(key.toString());
        DoubleWritable newvalue = new DoubleWritable(Math.log10(filenum/(wordnum+1)));
        context.write(newkey,newvalue);
    }
}
