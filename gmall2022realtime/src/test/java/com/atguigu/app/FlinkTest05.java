package com.atguigu.app;

import com.atguigu.bean.WaterSensor;
import com.atguigu.bean.WaterSensor2;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class FlinkTest05 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //默认的状态是一直保留
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));
        System.out.println(tableEnv.getConfig().getIdleStateRetention());
        //左流
        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop102", 9990);
        SingleOutputStreamOperator<WaterSensor> map = dataStreamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] fields = value.split(",");

                return new WaterSensor(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
            }
        });



        //右流
        DataStreamSource<String> dataStreamSource1 = env.socketTextStream("hadoop102", 9991);
        SingleOutputStreamOperator<WaterSensor2> map1 = dataStreamSource1.map(new MapFunction<String, WaterSensor2>() {
            @Override
            public WaterSensor2 map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor2(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });
        tableEnv.createTemporaryView("w1",map);
        tableEnv.createTemporaryView("w2",map1);

        //左表:onCreateAndWrite  右表:onCreateAndWrite(TTL的几种用法,时间到就过期)
       // tableEnv.executeSql("select w1.id,w1.ts,w2.vc from w1 join w2 on w1.id=w2.id").print();
        //左表:onReadAndWrite(只要一直有数据输入关联,过期时间就一直更新为0,继续倒计时)  右表:onCreateAndWrite(倒计时后过期)
        //tableEnv.executeSql("select w1.id,w1.ts,w2.vc from w1 left join w2 on w1.id=w2.id").print();

        //左表:onReadAndWrite(只要一直有数据输入关联,就一直不过期)  右表:onCreateAndWrite(倒计时后过期)
        //tableEnv.executeSql("select w1.id,w1.ts,w2.vc from w1 right join w2 on w1.id=w2.id").print();

        //左表:onReadAndWrite    右表:onReadAndWrite(两边都是一直有数据,就会一直更新ttl等待时间)
        tableEnv.executeSql("select w1.id,w1.ts,w2.vc from w1 full join w2 on w1.id=w2.id ").print();




    }
}
