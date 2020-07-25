package classification.KNNModel;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * ji suan ju li
 */
public class DistanceUtils {
    /**
     * 计算余弦距离
     * @param train
     * @param test
     * @return
     */
    public static double cosineDistance(Map<String, Double> train, Map<String, Double> test) {
        double vec = 0, testAbs = 0, trainAbs = 0;
        Set<Map.Entry<String, Double>> testSet = test.entrySet();
        for (Map.Entry<String, Double> me : testSet) {
            if (train.containsKey(me.getKey())) {
                vec += me.getValue() * train.get(me.getKey());
            }
            testAbs += me.getValue() * me.getValue();
        }
        testAbs = Math.sqrt(testAbs);

        Set<Map.Entry<String, Double>> trainSet = train.entrySet();
        for (Map.Entry<String, Double> me : trainSet) {
            trainAbs += me.getValue() * me.getValue();
        }
        trainAbs = Math.sqrt(trainAbs);

        return vec / (testAbs * trainAbs);
    }
}
