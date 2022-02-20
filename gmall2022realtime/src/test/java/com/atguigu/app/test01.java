package com.atguigu.app;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class test01 {
    public static void main(String[] args) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long time = sdf.parse("2022-02-15 00:04:05").getTime();
        System.out.println(time);
    }
}
