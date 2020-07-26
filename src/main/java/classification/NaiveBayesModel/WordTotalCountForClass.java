package classification.NaiveBayesModel;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import preprocess.TextTokenizer;


import java.io.IOException;
import java.util.List;

class WholeFileInputFormat extends FileInputFormat<NullWritable, BytesWritable> {

    @Override
    public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        WholeRecordReader recordReader = new WholeRecordReader();
        recordReader.initialize(inputSplit, taskAttemptContext);
        return recordReader;
    }
}

class WholeRecordReader extends RecordReader<NullWritable, BytesWritable> {
    BytesWritable value = new BytesWritable();
    boolean isProcess = false;
    FileSplit split;
    Configuration configuration;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        this.split = (FileSplit)inputSplit;
        configuration = taskAttemptContext.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!isProcess) {
            byte[] buf = new byte[(int) split.getLength()];

            FileSystem fs = null;
            FSDataInputStream fsDataInputStream = null;

            Path path = split.getPath();
            fs = path.getFileSystem(configuration);
            fsDataInputStream = fs.open(path);
            IOUtils.readFully(fsDataInputStream, buf, 0, buf.length);
            value.set(buf, 0, buf.length);

            fs.close();
            fsDataInputStream.close();
            isProcess = true;
            return true;
        }
        return false;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return isProcess?1:0;
    }

    @Override
    public void close() throws IOException {

    }
}

/**
 * 类名 单词总数
 */
public class WordTotalCountForClass {
    public static class WordTotalCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text text = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            Path path = fileSplit.getPath();

            text.set(path.getParent().getName());
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            List<String> values = TextTokenizer.getInstance().tokenize(value.toString());
            int count = 0;
            for (String word : values) {
                count++;
            }
            context.write(text, new IntWritable(count));
        }
    }

    public static class PriorProbabilityReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable intWritable = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            intWritable.set(sum);
            context.write(key, intWritable);
        }
    }
}
