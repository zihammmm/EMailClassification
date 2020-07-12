package preprocess;

import org.junit.After;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

class TextTokenizerTest {
    private final static String TEST_FLIE_NAME = "src/main/resources/1234";

    @Test
    void tokenize() {
        TextTokenizer textTokenizer = TextTokenizer.getInstance();
        List<String> list = textTokenizer.tokenize(TEST_FLIE_NAME);
        List<String> expectedList = Arrays.asList("i", "jio", "josdf", "jnsaf", "jjgoa", "fsao", "laodf");
        // 检查是否会去除数字
        Assert.assertEquals(expectedList, list);
    }

    @After
    void tearDown() {

    }
}