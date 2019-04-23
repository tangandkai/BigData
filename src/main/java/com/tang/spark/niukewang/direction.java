package com.tang.spark.niukewang;

import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;


/**
 * 【问题】
 *
 * 牛牛去犇犇老师家补课，出门的时候面向北方，但是现在他迷路了。
 * 虽然他手里有一张地图，但是他需要知道自己面向哪个方向，请你帮帮他。
 *
 * 输入描述:
 *
 * 每个输入包含一个测试用例。
 * 每个测试用例的第一行包含一个正整数，表示转方向的次数N(N<=1000)。
 * 接下来的一行包含一个长度为N的字符串，由L和R组成，L表示向左转，R表示向右转。
 * 输出描述:
 *
 * 输出牛牛最后面向的方向，N表示北，S表示南，E表示东，W表示西。
 * 输入例子1:
 *
 * 3
 * LRR
 * 输出例子1:
 *
 * E
 */
public class direction {

    private static Map hashMap = null;
    public static void main(String[] args) {

        init();
        System.out.println(hashMap.get(cal()));
//        System.out.println(-11%4);

    }

    /**
     * 使用map存储方向
     */
    private static void init(){
        hashMap = new HashMap();
        hashMap.put(0,'N');
        hashMap.put(1,'E');
        hashMap.put(2,'S');
        hashMap.put(3,'W');

        hashMap.put(-1,'W');
        hashMap.put(-2,'S');
        hashMap.put(-3,'E');
    }

    /**
     * 计算
     * @return
     */
    private static int cal(){
        int n = 0;
        int turnNumber = 0;
        Scanner scan = new Scanner(System.in);
        while(scan.hasNextLine()){
            String value = scan.nextLine();
            n++;
            if(n==1){
                try{
                    Integer.parseInt(value);
                }catch (Exception e){
                    throw new RuntimeException("请输入正正整数！");
                }
            }
            else {
                char[] array = value.toLowerCase().toCharArray();
                for (char x:array){
                    if (x=='l'){
                        turnNumber -= 1;
                    }
                    if (x=='r'){
                        turnNumber += 1;
                    }
                    else {;}
                }
                return (turnNumber)%4;
            }

        }
        return -1;
    }

}
