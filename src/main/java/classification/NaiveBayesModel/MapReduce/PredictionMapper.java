package classification.NaiveBayesModel.MapReduce;

import Utils.ProbabilityUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import preprocess.TextTokenizer;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static classification.NaiveBayesModel.Prediction.*;

public class PredictionMapper extends Mapper<NullWritable, Text, Text, Text> {
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //TODO:读文件

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



