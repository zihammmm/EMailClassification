package classification.NaiveBayesModel;

import Utils.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class NBMMain {
    public static void main(String[] args) throws IOException {
        Configuration configuration = new Configuration();
        String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
        if (otherArgs.length < 3) {
            System.err.println("args length wrong\n");
            System.exit(-1);
        }
        Path[] paths = FileUtils.folder(args[0], configuration);
        //TODO:补充输入输出

        /**
         * 统计每个类下的单词数
         * 输入：
         * 输出格式：
         * 类名1   单词数
         * 类名2  单词数
         */
        for (Path path : paths) {
            Job job = Job.getInstance(configuration, "NBM-wordTotalCount");
            job.setJarByClass(NBMMain.class);
            job.setMapperClass(WordTotalCountForClass.WordTotalCountMapper.class);
            job.setReducerClass(WordTotalCountForClass.PriorProbabilityReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

        }

        /**
         * 统计在类C下，单词X出现的次数
         * 输入：所有
         * 输出格式：
         * 类名1#单词1    次数
         * 类名1#单词1  次数
         */
        Job job2 = Job.getInstance(configuration, "NBM-wordCount");

        /**
         * 朴素贝叶斯
         * 输入：上面的输出
         * 输出格式：
         * 文件名  预测的类名
         */

        //初始化，不可省略
        Prediction prediction = new Prediction();
    }
}
