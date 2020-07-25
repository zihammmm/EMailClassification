package classification.NaiveBayesModel;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * 预测
 */
public class Prediction {
    /**
     *
     */
    public static class PredictionMapper extends Mapper<NullWritable, Text, Text, Text> {
        private Text text = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();

            Path path = fileSplit.getPath();
            text.set(path.getName() + "&" + path.getParent().getName());
        }

        @Override
        protected void map(NullWritable key, Text value, Context context) throws IOException, InterruptedException {
            Text text = new Text();

        }
    }

    public static class PredictionReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        }
    }
}
