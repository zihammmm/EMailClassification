package weight;

/**
 * @author zihan
 * 计算文本特征词的权重
 */
public class TFIDF {
    public static double tfidf(int count, int fileTotalNumber, int fileNumber) {
        return tf(count, fileTotalNumber) * idf(fileTotalNumber, fileNumber);
    }

    private static double tf(int count, int totalWordsNum) {
        return count * 1.0 / totalWordsNum;
    }

    private static double idf(int totalFileNumber, int fileNumber) {
        return Math.log(totalFileNumber * 1.0 / (fileNumber + 1));
    }
}
