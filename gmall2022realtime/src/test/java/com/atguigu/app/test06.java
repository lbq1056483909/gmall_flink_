package com.atguigu.app;

import com.atguigu.bean.WaterSensor;
import com.atguigu.bean.WaterSensor2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class test06 {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        System.out.println(tableEnv.getConfig().getIdleStateRetention());
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

        SingleOutputStreamOperator<WaterSensor> waterSensorDS1 = env.socketTextStream("hadoop102", 9990)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new WaterSensor(fields[0],
                            new Long(fields[1]),
                            new Double(fields[2]));
                });

        SingleOutputStreamOperator<WaterSensor2> waterSensorDS2 = env.socketTextStream("hadoop102", 9991)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new WaterSensor2(fields[0],
                            new Long(fields[1]),
                            new Double(fields[2]));
                });


        tableEnv.createTemporaryView("w1", waterSensorDS1);
        tableEnv.createTemporaryView("w2", waterSensorDS2);

        //左表:onCreateAndWrite  右表:onCreateAndWrite
        tableEnv.executeSql("select w1.id,w1.ts,w2.vc from w1 join w2 on w1.id=w2.id").print();

        //左表:onReadAndWrite    右表:onCreateAndWrite
        //tableEnv.executeSql("select w1.id,w1.ts,w2.vc from w1 left join w2 on w1.id=w2.id").print();

        //左表:onCreateAndWrite  右表:onReadAndWrite
        //tableEnv.executeSql("select w1.id,w1.ts,w2.vc from w1 right join w2 on w1.id=w2.id").print();

        //左表:onReadAndWrite    右表:onReadAndWrite
        //tableEnv.executeSql("select w1.id,w1.ts,w2.vc from w1 full join w2 on w1.id=w2.id").print();

    }

}
