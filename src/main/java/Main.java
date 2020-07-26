
import TFIDF.*;
import classification.KNNModel.KNN;
import classification.KNNModel.KNNMain;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.net.URI;

public class Main {
//    private static Path[] folder(String infolder,Configuration conf) throws IOException {
//        FileSystem temp = FileSystem.get(URI.create(infolder),conf);
//        FileStatus[] res = temp.listStatus(new Path(infolder));
//        return FileUtil.stat2Paths(res);
//    }
//
//    private static int getnum(String infolder,Configuration conf) throws IOException{
//        int filenum = 0;
//        FileSystem temp = FileSystem.get(URI.create(infolder),conf);
//        FileStatus[] res = temp.listStatus(new Path(infolder));
//        Path[] pathlist = FileUtil.stat2Paths(res);
//        for(Path path: pathlist)
//            filenum += temp.listStatus(path).length;
//        return filenum;
//    }
//
//    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
//        Configuration configuration = new Configuration();
//        String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
//        if (otherArgs.length < 2) {
//            System.err.println("args length wrong\n");
//            System.exit(2);
//        }
//        Path[] trainfolder = folder(args[0],configuration);
//        for(Path path : trainfolder)
//        {
//            Job job = Job.getInstance(configuration, "tf");
//            job.setJarByClass(Main.class);
//            job.setMapperClass(TFMapper.class);
//            job.setCombinerClass(EMailCombiner.class);
//            job.setReducerClass(TFReducer.class);
//
//            job.setMapOutputKeyClass(Text.class);
//            job.setMapOutputValueClass(DoubleWritable.class);
//
//            job.setOutputKeyClass(NullWritable.class);
//            job.setOutputValueClass(Text.class);
//
//            String[] temp = path.toString().split("/");
//
//            job.setProfileParams(temp[temp.length-1]);
//            FileInputFormat.addInputPath(job, path);
//            FileOutputFormat.setOutputPath(job, new Path(args[1]+"/tf/"+temp[temp.length-1]));
//            job.waitForCompletion(true);
//        }
//
//        Configuration configuration2 = new Configuration();
//        Job job2 = Job.getInstance(configuration2, "idf");
//        job2.setJarByClass(Main.class);
//        job2.setMapperClass(IDFMapper.class);
//        job2.setReducerClass(IDFReducer.class);
//
//        job2.setOutputKeyClass(Text.class);
//        job2.setOutputValueClass(DoubleWritable.class);
//
//        Integer filenum = getnum(args[0],configuration2);
//        job2.setProfileParams(filenum.toString());
//
//        Path[] trainfolder2 = folder(args[1]+"/tf",configuration);
//        for(Path path2 : trainfolder2)
//        {
//            FileInputFormat.addInputPath(job2,path2);
//        }
//        FileOutputFormat.setOutputPath(job2,new Path(args[1]+"/idf"));
//        job2.waitForCompletion(true);
//
//
//        Configuration configuration3 = new Configuration();
//        Job job3 = Job.getInstance(configuration3, "tf-idf");
//        job3.setJarByClass(Main.class);
//        job3.setMapperClass(TFIDFMapper.class);
//        job3.setReducerClass(TFIDFReducer.class);
//
//        job3.setMapOutputKeyClass(Text.class);
//        job3.setMapOutputValueClass(Text.class);
//        job3.setOutputKeyClass(NullWritable.class);
//        job3.setOutputValueClass(Text.class);
//
//        job3.setProfileParams(args[1]+"/idf/part-r-00000");
//
//        Path[] trainfolder3 = folder(args[1]+"/tf",configuration);
//        for(Path path3 : trainfolder3)
//        {
//            FileInputFormat.addInputPath(job3,path3);
//        }
//        FileOutputFormat.setOutputPath(job3,new Path(args[1]+"/tf-idf"));
//        job3.waitForCompletion(true);
//    }

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