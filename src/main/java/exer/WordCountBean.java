package exer;

import org.apache.flink.table.expressions.In;

/**
 * @author zhengyonghong
 * @create 2020--12--18--8:43
 */
public class WordCountBean {
    private String word;
    private Integer count1;

    public WordCountBean() {
    }

    public WordCountBean(String word, Integer count1) {
        this.word = word;
        this.count1 = count1;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public Integer getCount1() {
        return count1;
    }

    public void setCount1(Integer count1) {
        this.count1 = count1;
    }

    @Override
    public String toString() {
        return "WordCountBean{" +
                "word='" + word + '\'' +
                ", count1=" + count1 +
                '}';
    }
}
