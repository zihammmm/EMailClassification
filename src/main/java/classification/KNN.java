package classification;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;


public class KNN {
    static class FileBean {
        String className;
        String fileName;

        public FileBean(String className, String fileName) {
            this.className = className;
            this.fileName = fileName;
        }

        public String getClassName() {
            return className;
        }
    }

    public static class KNNMapper extends Mapper<Object, Text, Text, Text> {
        private static final int K_NEIGHBOR = 10;
        private TreeMap<FileBean, TreeMap<String, Double>> train = new TreeMap<>();
        private TreeMap<FileBean, TreeMap<String, Double>> test = new TreeMap<>();
        /**
         *
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            FileSystem fs = FileSystem.get(context.getConfiguration());
            Path trainPath = new Path(cacheFiles[0]);
            FileStatus[] fileStatuses = fs.listStatus(trainPath);
            Path[] fullVec = FileUtil.stat2Paths(fileStatuses);
            for (Path p : fullVec) {
                //TODO:
                FileBean fileBean = new FileBean("className", "fileName");
                TreeMap<String, Double> tfidf = new TreeMap<>();
                try(BufferedReader in = new BufferedReader(new FileReader(p.toString()))) {
                    String msg;
                    while ((msg = in.readLine()) != null) {
                        String[] attr = msg.split("#");
                        tfidf.put(attr[0], Double.parseDouble(attr[1]));
                    }
                }catch (IOException e) {
                    e.printStackTrace();
                }
                train.put(fileBean, tfidf);
            }
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName();



        }

    }
}
