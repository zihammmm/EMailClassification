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
    private static TreeMap<FileBean, TreeMap<String, Double>> train = new TreeMap<>();
    private ArrayList<FileBean> result = new ArrayList<>();

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

        /**
         *
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName();


            try(BufferedReader in = new BufferedReader(new FileReader(fileName))) {
                String msg;
                while ((msg = in.readLine()) != null) {
                    String[] keyValue = msg.split("\\s+");
                    String name = keyValue[0].split("\\.")[0];

                    FileBean fileBean = new FileBean(name.split("#")[0], name.split("#")[1]);
                    TreeMap<String, Double> singleFileTFIDF = new TreeMap<>();
                    for (int i = 1; i < keyValue.length; i++) {
                        String[] unit = keyValue[i].split(":");
                        singleFileTFIDF.put(unit[0], Double.parseDouble(unit[1]));
                    }
                    train.put(fileBean, singleFileTFIDF);
                }
            }catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] keyValue = value.toString().split("\\s+");
            String fileName = keyValue[0].split("\\.")[0].split("#")[1];
            TreeMap<String, Double> tfidf = new TreeMap<>();
            for (int i = 1; i < keyValue.length; i++) {
                String[] unit = keyValue[i].split(":");
                tfidf.put(unit[0], Double.parseDouble(unit[1]));
            }

        }



    }
}
