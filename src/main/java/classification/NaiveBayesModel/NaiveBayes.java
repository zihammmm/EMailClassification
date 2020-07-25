package classification.NaiveBayesModel;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class NaiveBayes {
    public static class NBMapper extends Mapper<Object, Text, Text, Text> {

    }

    public static class NBReducer extends Reducer<Text, Text, Text, LongWritable> {

    }
}
