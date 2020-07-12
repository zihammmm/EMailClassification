package mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import preprocess.TextTokenizer;

import java.io.IOException;
import java.util.List;

public class EMailMapper extends Mapper<Object, Text, Text, IntWritable> {
    private Text word = new Text();
    private IntWritable count = new IntWritable(1);

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit)context.getInputSplit();
        String fileName = fileSplit.getPath().toString();
        List<String> list = TextTokenizer.getInstance().tokenize(fileName);
        for (String string : list) {
            word.set(string);
            context.write(word, count);
        }
    }
}
