package com.tang.spark.niukewang;



public class wangyitest {

    int count = 21;
    public void count() {
        System.out.println(++count);
    }
    public static void main(String[] args) {
        String str="o";
        str.concat("abc");
        str.concat("123");
        System.out.println(str);

        new wangyitest().count();
        new wangyitest().count();
    }
}
