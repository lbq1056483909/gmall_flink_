package com.atguigu.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.bean.OrderDetail;
import com.atguigu.app.bean.OrderInfo;
import com.atguigu.app.bean.OrderWide;
import com.atguigu.app.func.DimAsyncFunction;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;

import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName OrderWideApp
 * @Description TODO
 * @Author 10564
 * @Date 2022/2/14 20:42
 * @Version 1.0
 */
public class OrderWideApp {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //1.1设置状态后端
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/dwd_log/ck"));
//        //1.2 开启CK
//        env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        //2.读取Kafka订单和订单明细主题数据dwd_order_info  dwd_order_detail
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_group";
        //从kafka获取事实表订单表和订单明细表两条流数据
        FlinkKafkaConsumer<String> orderkafkaSource = MyKafkaUtil.getKafkaSource(orderInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> orderdetailkafkasource = MyKafkaUtil.getKafkaSource(orderDetailSourceTopic, groupId);
        //流中order添加到kafka source
        DataStreamSource<String> orderInfoDS = env.addSource(orderkafkaSource);
        //流中order_tail添加到kafka source
        DataStreamSource<String> orderDetailDS = env.addSource(orderdetailkafkasource);

//        orderInfoDS.print();
//        orderDetailDS.print();

        //todo 将每行数据转换为Javabean,提取时间戳生成watermark
        SingleOutputStreamOperator<OrderInfo> orderInfowatermark = (SingleOutputStreamOperator<OrderInfo>) orderInfoDS.map(new MapFunction<String, OrderInfo>() {
            @Override
            public OrderInfo map(String value) throws Exception {
                //将json
                OrderInfo orderInfo = JSON.parseObject(value, OrderInfo.class);
                //通过javabean里面的get set方法来获取
                String create_time = orderInfo.getCreate_time();
                String[] dateTimeArr = create_time.split(" ");
                //给增加的字段通过creare赋值
                orderInfo.setCreate_date(dateTimeArr[0]);
                orderInfo.setCreate_hour(dateTimeArr[1].split(":")[0]);
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                //将2022-02-15 00:04:05格式按照sfd格式,然后再转化为时间戳
                orderInfo.setCreate_ts(sdf.parse(create_time).getTime());
                //将字段都补齐后转换为orderInfo
                return orderInfo;
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy
                .<OrderInfo>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                    @Override
                    public long extractTimestamp(OrderInfo element, long recordTimestamp) {
                        //事件事件按照create_ts
                        return element.getCreate_ts();
                    }
                }));

        SingleOutputStreamOperator<OrderDetail> orderdetailwatermark = orderDetailDS.map(new MapFunction<String, OrderDetail>() {
            @Override
            public OrderDetail map(String value) throws Exception {
                OrderDetail orderDetail = JSON.parseObject(value, OrderDetail.class);
                String create_time = orderDetail.getCreate_time();
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                orderDetail.setCreate_ts(sdf.parse(create_time).getTime());
                return orderDetail;
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy
                .<OrderDetail>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                    @Override
                    public long extractTimestamp(OrderDetail element, long recordTimestamp) {
                        return element.getCreate_ts();
                    }
                }));
        //todo 4.双流join
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderInfowatermark.keyBy(OrderInfo::getId)
                .intervalJoin(orderdetailwatermark.keyBy(OrderDetail::getOrder_id))
                .between(Time.seconds(-5), Time.seconds(5))//生产环境中,如果不想丢失数据,就设置成最大的延时'
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo left, OrderDetail right, Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(left, right));
                    }
                });
        //打印测试双流join后的数据个数,检查搜索orderWideDS的个数,然后对比mysql中的个数
        //orderWideDS.print("orderWideDS>>>>>>>>>>>");
        // todo 5.关联维表
//        orderWideDS.map(new RichMapFunction<OrderWide, OrderWide>() {
//
//            @Override
//            public void open(Configuration parameters) throws Exception {
//
//            }
//
//            @Override
//            public OrderWide map(OrderWide value) throws Exception {
//                //关联用户维度
//                //关联地区维度
//                //关联SKU,SPU,TM,Category维度
//                return null;
//            }
//        })

        //todo 5.1 用户维度
        SingleOutputStreamOperator<OrderWide> orderWideWithUserDS = AsyncDataStream.unorderedWait(orderWideDS, new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {
            @Override
            public String getKey(OrderWide orderWide) {
                //获取订单表和订单明细表关联后的user_id,然后传递进去,通过这个user_id,来查找用户维度表其他信息
                return orderWide.getUser_id().toString();
            }

            //就是补全大宽里面所缺的字段
            @Override   //这边的jsonObject就是查询到用户表的信息
            public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                //获取用户维度表的GENDER字段对应的值,并添加进去
                orderWide.setUser_gender(jsonObject.getString("GENDER"));
                String birthday = jsonObject.getString("BIRTHDAY");
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                long ts = System.currentTimeMillis();

                long age = (ts - sdf.parse(birthday).getTime()) / (1000L * 60 * 60 * 24 * 365);

                orderWide.setUser_age((int) age);

            }
        }, 100, TimeUnit.SECONDS);
        orderWideWithUserDS.print("orderWideWithUserDS>>>>>>>>");
        //TODO 5.2 地区维度
        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDS = AsyncDataStream.unorderedWait(orderWideWithUserDS,
                new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        //获取表的主键字段
                        return orderWide.getProvince_id().toString();
                    }
                    //jsonObject就是得到的维度表信息,然后将维度表里面每一个字段的值取出来,赋值为orderwide
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) {
                        orderWide.setProvince_name(jsonObject.getString("NAME"));
                        orderWide.setProvince_area_code(jsonObject.getString("AREA_CODE"));
                        orderWide.setProvince_iso_code(jsonObject.getString("ISO_CODE"));
                        orderWide.setProvince_3166_2_code(jsonObject.getString("ISO_3166_2"));
                    }
                },
                100,
                TimeUnit.SECONDS);

        //TODO 5.3 SKU
        SingleOutputStreamOperator<OrderWide> orderWideWithSkuDS = AsyncDataStream.unorderedWait(
                orderWideWithProvinceDS, new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setSku_name(jsonObject.getString("SKU_NAME"));
                        orderWide.setCategory3_id(jsonObject.getLong("CATEGORY3_ID"));
                        orderWide.setSpu_id(jsonObject.getLong("SPU_ID"));
                        orderWide.setTm_id(jsonObject.getLong("TM_ID"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSku_id());
                    }
                }, 60, TimeUnit.SECONDS);

        //TODO 5.4 SPU
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDS = AsyncDataStream.unorderedWait(
                orderWideWithSkuDS, new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setSpu_name(jsonObject.getString("SPU_NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSpu_id());
                    }
                }, 60, TimeUnit.SECONDS);

        //TODO 5.5 TM
        SingleOutputStreamOperator<OrderWide> orderWideWithTmDS = AsyncDataStream.unorderedWait(
                orderWideWithSpuDS, new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setTm_name(jsonObject.getString("TM_NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getTm_id());
                    }
                }, 60, TimeUnit.SECONDS);

        //TODO 5.6 Category
        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3DS = AsyncDataStream.unorderedWait(
                orderWideWithTmDS, new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setCategory3_name(jsonObject.getString("NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getCategory3_id());
                    }
                }, 60, TimeUnit.SECONDS);

        orderWideWithCategory3DS.print("orderWideWithCategory3DS>>>>>>>>");

        //TODO 6.将数据写出到Kafka
       orderWideWithCategory3DS.map(JSON::toJSONString)
               .addSink(MyKafkaUtil.getKafkaSink(orderWideSinkTopic));

        //TODO 7.启动任务
        env.execute("OrderWideApp");


    }
}

