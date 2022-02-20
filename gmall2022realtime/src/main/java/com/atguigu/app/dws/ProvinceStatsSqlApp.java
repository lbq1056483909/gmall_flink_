package com.atguigu.app.dws;

import com.atguigu.app.bean.ProvinceStats;
import com.atguigu.utils.ClickHouseUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @ClassName ProvinceStatsSqlApp
 * @Description TODO
 * @Author 10564
 * @Date 2022/2/18 13:10
 * @Version 1.0
 */
public class ProvinceStatsSqlApp {
    public static void main(String[] args) throws Exception {
        //todo 1.定义table流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //CK
        //        env.setStateBackend(new FsStateBackend("hdfs://"));
        //        env.enableCheckpointing(5000L);
        //        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        //        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        //        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        //        env.getCheckpointConfig().setCheckpointInterval(10000L);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //todo 2.创建从kafka读取数据的表,并且提取事件事件生成waterMark
          //2.1先根据字段创建表 2.2将时间转换为timestamp3 2.3.生成watermark
        //配置kafka信息
        String topic="dwm_order_wide";
        String groupId="province_stats_210826";

        tableEnv.executeSql("create table order_wide(\n" +
                "\torder_id bigint,\n" +
                "\tprovince_id bigint,\n" +
                "\tsplit_total_amount DECIMAL,\n" +
                "\tprovince_name String,\n" +
                "\tprovince_area_code String,\n" +
                "\tprovince_iso_code String,\n" +
                "\tprovince_3166_2_code String,\n" +
                "\tcreate_time String,\n" +
                "\trt as TO_TIMESTAMP(create_time), \n" +
                "\tWATERMARK FOR rt AS rt - INTERVAL '2' SECOND" +
                ")with(" + MyKafkaUtil.getKafkaDDL(topic, groupId));

        //todo 3.通过sql查询出结果表(开窗,计算结果)(因为要算窗口大小为10的数据,所以需要开窗)
        Table resultTable = tableEnv.sqlQuery("select\n" +
                "\tDATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' second),'yyyy-MM-dd HH:mm:ss') stt,\n" +
                "\tDATE_FORMAT(TUMBLE_END(rt, INTERVAL '10' second),'yyyy-MM-dd HH:mm:ss') edt,\n" +
                "\tprovince_id,\n" +
                "\tprovince_name,\n" +
                "\tprovince_area_code,\n" +
                "\tprovince_iso_code ,\n" +
                "\tprovince_3166_2_code,\t\n" +
                "\tcount(distinct (order_id)) order_count,\n" +
                "\tsum(split_total_amount) order_amount,\n" +
                "\tUNIX_TIMESTAMP()*1000 ts\n" +
                "from order_wide\n" +
                "group by \n" +
                "\tprovince_id,\n" +
                "\tprovince_name,\n" +
                "\tprovince_area_code,\n" +
                "\tprovince_iso_code ,\n" +
                "\tprovince_3166_2_code,\n" +
                "\tTUMBLE(rt, INTERVAL '10' second)");
        //流的类型就是得到的结果类型那样
        //todo 4.把结果表转换为数据流()格式为地区宽表类型
        DataStream<ProvinceStats> provinceStatsDatastream = tableEnv.toAppendStream(resultTable, ProvinceStats.class);


        //todo 5.把数据流写入目标数据库
        provinceStatsDatastream.print(">>>>>>>>>>>>");
        provinceStatsDatastream.addSink(ClickHouseUtil.getSinkFunction("insert into province_stats_210826 values(?,?,?,?,?,?,?,?,?,?)"));

        //todo 6.启动任务
        env.execute("ProvinceStatsSqlApp");




    }
}

