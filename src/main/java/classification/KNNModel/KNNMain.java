package classification.KNNModel;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class KNNMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 3) {
            System.err.println("args length wrong\n");
            System.exit(2);
        }
        Job knnjob = Job.getInstance(conf, "knn");
        knnjob.setJarByClass(KNNMain.class);
        knnjob.setMapperClass(KNN.KNNMapper.class);
        knnjob.setProfileParams(args[0]);
        knnjob.setMapOutputKeyClass(Text.class);
        knnjob.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(knnjob,new Path(args[1]));
        FileOutputFormat.setOutputPath(knnjob,new Path(args[2]));

        knnjob.waitForCompletion(true);
    }
}
