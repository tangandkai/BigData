package com.tang.spark.flink.dataSet_j;

public class WC<Key,Value> {

    private Key word;
    private Value count;

    public WC() {
    }

    public WC(Key word, Value count) {
        this.word = word;
        this.count = count;
    }

    public Key getWord() {
        return word;
    }

    public void setWord(Key word) {
        this.word = word;
    }

    public Value getCount() {
        return count;
    }

    public void setCount(Value count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "WC{" +
                "word=" + word +
                ", count=" + count +
                '}';
    }
}
