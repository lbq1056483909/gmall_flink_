package com.atguigu.app.func;

import com.alibaba.fastjson.JSONObject;


//这个异步维表查询的方法适用于各种维表的查询，用什么条件查，查出来的结果如何合并到数据流对象中，需要使用者自己定义。
//这就是自己定义了一个接口DimJoinFunction<T>包括两个方法。
public interface DimJoinFunction<T> {
    //也可以写到类中,但是和抽象方法混在一起不清楚,单独拿出来,写在接口里面
    //获取数据中的所要关联维度的主键
    String getKey(T input);

    //关联事实数据和维度数据
    void join(T input, JSONObject dimInfo) throws Exception;

}
