package classification.KNNModel;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

/**
 * KNN模型
 */
public class KNN {
    /**
     * 用于保存训练集数据
     */
    private static HashMap<FileBean, HashMap<String, Double>> train = new HashMap<>();
    private static final double MIN_DIFF = 10E-5;

    /**
     * 训练样本的状态
     * 保存类名，文件名，还有与测试样本的距离
     */
    static class FileBean {
        String className;
        String fileName;
        double distance;

        public FileBean(String className, double distance) {
            this.className = className;
            this.distance = distance;
        }

        public FileBean(String className, String fileName) {
            this.className = className;
            this.fileName = fileName;
        }

        public String getClassName() {
            return className;
        }

        public void setDistance(double distance) {
            this.distance = distance;
        }

        public double getDistance() {
            return distance;
        }
    }

    public static class KNNMapper extends Mapper<Object, Text, Text, Text> {
        private static final int K_NEIGHBOR = 10;

        /**
         * 读取训练集的数据
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            String idffilefolder = context.getProfileParams();
            FileSystem temp = FileSystem.get(URI.create(idffilefolder), context.getConfiguration());
            FileStatus[] res = temp.listStatus(new Path(idffilefolder));
            Path[] paths = FileUtil.stat2Paths(res);

            for (Path p : paths) {
                FSDataInputStream inStream = FileSystem.get(context.getConfiguration()).open(p);
                while (inStream.available() > 0) {
                    String msg = inStream.readLine();
                    String[] keyValue = msg.split("\\s+");
                    String name = keyValue[0].split("\\.")[0];

                    FileBean fileBean = new FileBean(name.split("#")[0], name.split("#")[1]);
                    HashMap<String, Double> singleFileTFIDF = new HashMap<>();
                    for (int i = 1; i < keyValue.length; i++) {
                        String[] unit = keyValue[i].split(":");
                        singleFileTFIDF.put(unit[0], Double.parseDouble(unit[1]));
                    }
                    train.put(fileBean, singleFileTFIDF);
                }
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

            //存储在一个优先级队列中
            Queue<FileBean> topK = new PriorityQueue<>(comparator);
            for (FileBean word : train.keySet()) {
                word.setDistance(DistanceUtils.cosineDistance(train.get(word), tfidf));
                topK.add(word);
            }

            TreeMap<String, Integer> vote = new TreeMap<>();
            //取出前k个，投票
            for (int i = 0; i < K_NEIGHBOR; i++) {
                String className = Objects.requireNonNull(topK.poll()).getClassName();
                if (!vote.containsKey(className)) {
                    vote.put(className, 1);
                }else {
                    int num = vote.get(className);
                    vote.remove(className);
                    vote.put(className, num + 1);
                }
            }

            int maxNum = -1;
            String targetClassName = null;
            for (String className : vote.keySet()) {
                if (maxNum < vote.get(className)) {
                    maxNum = vote.get(className);
                    targetClassName = className;
                }
            }
            //输出类名
            context.write(new Text(targetClassName), new Text(fileName));
        }

        /**
         * 比较器，降序排列
         */
        static Comparator<FileBean> comparator = (o1, o2) -> {
            double diff = o1.getDistance() - o2.getDistance();
            return Double.compare(MIN_DIFF, Math.abs(diff));
        };


    }
}
