package classification;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * ji suan ju li
 */
public class DistanceUtils {
    public static double cosineDistance(Map<String, Double> train, Map<String, Double> test) {
        double vec = 0, testAbs = 0, trainAbs = 0;
        Set<Map.Entry<String, Double>> testSet = test.entrySet();
        for (Iterator<Map.Entry<String, Double>> it = testSet.iterator(); it.hasNext();) {
            Map.Entry<String, Double> me = it.next();
            if (train.containsKey(me.getKey())) {
                vec += me.getValue() * train.get(me.getKey());
            }
            testAbs += me.getValue() * me.getValue();
        }
        testAbs = Math.sqrt(testAbs);

        Set<Map.Entry<String, Double>> trainSet = train.entrySet();
        for (Iterator<Map.Entry<String, Double>> it = trainSet.iterator(); it.hasNext();) {
            Map.Entry<String, Double> me = it.next();
            trainAbs += me.getValue() * me.getValue();
        }
        trainAbs = Math.sqrt(trainAbs);

        return vec / (testAbs * trainAbs);
    }
}
