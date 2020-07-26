package classification.NaiveBayesModel;

import Utils.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import preprocess.TextTokenizer;

import java.io.IOException;
import java.util.*;

/**
 * 预测
 */
public class Prediction {
    /**
     * 单词总数 （允许重复）
     */
    private static double wordTotalNum = 0;

    /**
     * 单词表大小多个算一个
     */
    private static double wordListSize = 0;

    /**
     * 类名列表
     */
    private static List<String> classList = new LinkedList<>();

    /**
     * 类c下的单词总数
     */
    private static HashMap<String, Integer> wordTotalNumForClass = new HashMap<>();

    /**
     * 类c下的单词出现的次数
     */
    private static HashMap<String, HashMap<String, Integer>> wordNumForClass = new HashMap<>();

    /**
     * 先验概率
     */
    static HashMap<String, Double> priorProbability = new HashMap<>();

    /**
     * 类条件概率
     */
    private static HashMap<String, Double> condProbability = new HashMap<>();

    /**
     * P(C）
     */

    public Prediction() {

        //先计算先验概率 计算单词总数
        for (String className : wordTotalNumForClass.keySet()) {
            priorProbability.put(className, ProbabilityUtils.calPriorProb(className));
            wordTotalNum += wordTotalNumForClass.get(className);
            classList.add(className);
        }

        Set<String> wordList = new HashSet<>();

        //类条件概率
        for (String className : wordNumForClass.keySet()) {
            HashMap<String, Integer> values = wordNumForClass.get(className);
            for (String word : values.keySet()) {
                condProbability.put(className + "#" + word, ProbabilityUtils.calConProb(word, className));
                wordList.add(word);
            }
        }

        wordListSize = wordList.size();
    }

    public static class ProbabilityUtils {
        public static double calPriorProb(String className) {
            return wordTotalNumForClass.get(className) / wordTotalNum;
        }

        public static double calConProb(String word, String className){
            HashMap<String, Integer> count = wordNumForClass.get(className);
            if (count.get(word) == null) {
                return Math.log(1 / (wordTotalNumForClass.get(className) + wordListSize));
            }else {
                return Math.log((count.get(word) + 1) / (wordTotalNumForClass.get(className) + wordListSize));
            }
        }
    }



    public static class PredictionMapper extends Mapper<NullWritable, Text, Text, Text> {

        /**
         * 计算概率
         * 输出为：
         * 文件名  类名#概率
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(NullWritable key, Text value, Context context) throws IOException, InterruptedException {
            List<String> words = TextTokenizer.getInstance().tokenizeOneLine(value.toString());
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            Text text = new Text(fileSplit.getPath().getName());    //TODO:检查文件名

            for (String className : classList) {
                double pro = priorProbability.get(className);
                for (String word : words) {
                    if (condProbability.get(className + "#" + word) != null) {
                        pro *= condProbability.get(className + "#" + word);
                    }
                }
                context.write(text, new Text(className + "#" + pro));
            }

        }
    }


    public static class PredictionReducer extends Reducer<Text, Text, Text, Text> {
        /**
         * 找出概率最大的类
         * 输出：
         * 文件名  类名
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String preClass = null;
            Double maxValue = Double.MIN_VALUE;
            for (Text value : values) {
                String[] args = value.toString().split("#");
                if (Double.compare(Double.parseDouble(args[1]), maxValue) > 0) {
                    maxValue = Double.parseDouble(args[1]);
                    preClass = args[0];
                }
            }
            context.write(key, new Text(preClass));
        }
    }
}
