package com.atguigu.app.func;


import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;

import com.atguigu.utils.DimUtil;
import com.atguigu.utils.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @ClassName DimAsyncFunction
 * @Description TODO
 * @Author 10564
 * @Date 2022/2/16 0:30
 * @Version 1.0
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimJoinFunction<T> {
    //声明线程池对象
    private ThreadPoolExecutor threadPoolExecutor;

    //声明属性(需要创建构造器,因为创建对象时需要将表名传进来)
    private String tableName;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        //初始化线程池,使用封装的线程池工具类
        threadPoolExecutor = ThreadPoolUtil.getInstance();
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        threadPoolExecutor.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    //1.根据表名和主键查询对应的维度信息(找到唯一的一条信息)
                                  //这个input是一个表名,从外面传递,将计算好的结果再传递进来
                    String pk = getKey(input);
                    //通过主键字段和表名和主键得到一条user_id唯一维度信息
                    JSONObject dimInfo = DimUtil.getDimInfo(connection, tableName, pk);
                    //2.将查询到的维度信息补充至原始数据(维度信息有用户的,地区的等等)
                    if (dimInfo != null) {
                        join(input, dimInfo);
                    }
                    //3.将数据写入流中
                    resultFuture.complete(Collections.singleton(input));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    @Override//这个input就是传进来的类型
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        System.out.println("TimeOut:" + input);

    }


}

