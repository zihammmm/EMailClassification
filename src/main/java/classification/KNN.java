package classification;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * KNN模型
 */
public class KNN {
    /**
     * 用于保存训练集数据
     */
    private static TreeMap<FileBean, TreeMap<String, Double>> train = new TreeMap<>();
    private ArrayList<FileBean> result = new ArrayList<>();
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
            Set<Map.Entry<FileBean, TreeMap<String, Double>>> trainSet = train.entrySet();

            //存储在一个优先级队列中
            Queue<FileBean> topK = new PriorityQueue<>(comparator);
            for (Map.Entry<FileBean, TreeMap<String, Double>> me : trainSet) {
                double retTFIDF = DistanceUtils.cosineDistance(me.getValue(), tfidf);
                me.getKey().setDistance(retTFIDF);
                topK.add(me.getKey());
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

            Set<Map.Entry<String, Integer>> selectBest = vote.entrySet();
            int maxNum = -1;
            String targetClassName = null;
            for (Map.Entry<String, Integer> me : selectBest) {
                if (maxNum < me.getValue()) {
                    maxNum = me.getValue();
                    targetClassName = me.getKey();
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
