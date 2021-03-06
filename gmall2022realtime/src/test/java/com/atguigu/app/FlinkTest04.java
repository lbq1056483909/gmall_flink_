package com.atguigu.app;

import com.atguigu.bean.WaterSensor;
import com.atguigu.bean.WaterSensor2;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import javax.xml.bind.annotation.W3CDomHandler;
import java.time.Duration;

/**
 * @ClassName FlinkTest04
 * @Description TODO
 * @Author 10564
 * @Date 2022/2/14 15:37
 * @Version 1.0
 */

public class FlinkTest04 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop102", 9990);
        SingleOutputStreamOperator<WaterSensor> map = dataStreamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] fields = value.split(",");

                return new WaterSensor(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
            }
        });
        SingleOutputStreamOperator<WaterSensor> watermarks = map.assignTimestampsAndWatermarks(WatermarkStrategy
                .<WaterSensor>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs();
                    }
                }));

        DataStreamSource<String> dataStreamSource1 = env.socketTextStream("hadoop102", 9991);
        SingleOutputStreamOperator<WaterSensor2> map1 = dataStreamSource1.map(new MapFunction<String, WaterSensor2>() {
            @Override
            public WaterSensor2 map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor2(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });
        SingleOutputStreamOperator<WaterSensor2> watermarks1 = map1.assignTimestampsAndWatermarks(WatermarkStrategy
                .<WaterSensor2>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor2>() {
                    @Override
                    public long extractTimestamp(WaterSensor2 element, long recordTimestamp) {
                        return element.getTs();
                    }
                }));


        //??????????????????,???????????????????????????????????????
        //1.????????????5s??????????????????,????????????5s??????????????????
        //??????????????????,watermaek????????????????????????????????????,??????????????????
        //??????+5s,??????+5s???????????????????????????
        SingleOutputStreamOperator<Tuple2<WaterSensor, WaterSensor2>> result =
                 watermarks.keyBy(WaterSensor::getId)
                .intervalJoin(watermarks1.keyBy(WaterSensor2::getId))
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<WaterSensor, WaterSensor2, Tuple2<WaterSensor, WaterSensor2>>() {
                    @Override
                    public void processElement(WaterSensor left, WaterSensor2 right, Context ctx, Collector<Tuple2<WaterSensor, WaterSensor2>> out) throws Exception {
                        out.collect(new Tuple2<>(left, right));
                    }
                });

        result.print();
        env.execute();


    }
}
