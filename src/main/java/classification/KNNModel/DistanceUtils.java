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
        for (String key : test.keySet()) {
            if (train.containsKey(key)) {
                vec += test.get(key) * train.get(key);
            }
            testAbs += test.get(key) * test.get(key);
        }
        testAbs = Math.sqrt(testAbs);

        for (String key : train.keySet()) {
            trainAbs += train.get(key) * train.get(key);
        }
        trainAbs = Math.sqrt(trainAbs);
        if (testAbs == 0 || trainAbs == 0) {
            return -1;
        }
        return vec / (testAbs * trainAbs);
    }
}
