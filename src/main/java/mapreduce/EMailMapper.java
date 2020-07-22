package mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.io.DoubleWritable;
import preprocess.TextTokenizer;

import java.io.IOException;
import java.util.List;

public class EMailMapper extends Mapper<Object, Text, Text, DoubleWritable> {
    private Text word = new Text();
    private DoubleWritable count = new DoubleWritable(1);

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit)context.getInputSplit();
        String fileName = fileSplit.getPath().toString();
        fileName = fileName.substring(5);
        List<String> list = TextTokenizer.getInstance().tokenize(fileName);
        fileName = fileSplit.getPath().getName();
        word.set("!#"+fileName);
        context.write(word,new DoubleWritable(list.size()));
        for (String string : list) {
            word.set(string+'#'+fileName);
            context.write(word, count);
        }
    }
}
