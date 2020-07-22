package mapreduce.TFIDF;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.io.DoubleWritable;

import java.io.IOException;

public class IDFMapper extends Mapper<Object, Text, Text, DoubleWritable>{
    private DoubleWritable count = new DoubleWritable(1);
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException{
        FileSplit fileSplit = (FileSplit)context.getInputSplit();
        Text word = new Text(value.toString().split("\t")[0]);
        context.write(new Text(word), count);
    }
}
