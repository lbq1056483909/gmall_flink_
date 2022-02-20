import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

/**
 * @ClassName FlinkCDC_DataStream_Deserializer
 * @Description TODO
 * @Author 10564
 * @Date 2022/1/24 0:39
 * @Version 1.0
 */

public class FlinkCDC_DataStream_Deserializer {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        //2.Flink-CDC将读取binlog的位置信息以状态的方式保存在CK,如果想要做到断点续传,需要从Checkpoint或者Savepoint启动程序
//        //2.1 开启Checkpoint,每隔5秒钟做一次CK
//        env.enableCheckpointing(5000L);
//        //2.2 指定CK的一致性语义
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        //2.3 设置任务关闭的时候保留最后一次CK数据
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        //2.4 指定从CK自动重启策略
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000L));
//        //2.5 设置状态后端
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flinkCDC"));
//        //2.6 设置访问HDFS的用户名
//        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //2.创建Flink_mySql-cdc的source
        //需要写泛型,将都过来的数据转为string类型进行解析
        DebeziumSourceFunction<String> sourceFunction = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .databaseList("gmall0826")
                .tableList("gmall0826.base_trademark")
                .username("root")
                .password("123456")
                .startupOptions(StartupOptions.initial())//标识初始化时只读取一次,后面从最新读,历史数据放到checkpoint
                .deserializer(new MyDeserialization())
                .build();

        DataStreamSource<String> stringDataStreamSource = env.addSource(sourceFunction);
        stringDataStreamSource.print();
        env.execute();
    }

    public static class MyDeserialization implements DebeziumDeserializationSchema<String> {

        @Override
        public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
            //1.创建一个JSONObject用来存放最终封装好的数据
            JSONObject result = new JSONObject();
            //2.获取数据库以及表名:原始数据里有SourceRecord和 ConnectRecord
            //重要数据基本都在ConnectRecord里面,SourceRecord继承于ConnectRecord,所以可以直接调用ConnectRecord里面的方法
            String topic = sourceRecord.topic();
            //原始数据里:topic是这个格式,直接用.切割来获取里面的数据.topic='mysql_binlog_source.gmall0826.base_trademark'
            String[] split = topic.split("\\.");
            //数据库名
            String database = split[1];
            //表名
            String tableName = split[2];
            //3.获取类型(这个方法需要记一下)
            Envelope.Operation operation = Envelope.operationFor(sourceRecord);
            //将获取到的类型tostring,并且转为小写字母
            String type = operation.toString().toLowerCase();
            //将create替换成insert,因为cdc2.0将插入数据类型变为了create
            if ("create".equals(type)) {
                type = "insert";
            }
            //4.获取数据(因为原始数据格式是struct,所以强转一下)
            Struct value = (Struct) sourceRecord.value();
            //5.获取before数据
            Struct structbefore = value.getStruct("before");
            //new一个放before数据的json
            JSONObject beforeJson = new JSONObject();
            if (structbefore != null) {
                //因为每个数据的字段都不一样,所以用schema,遍历每个字段
                Schema schema = structbefore.schema();
                //获取所有字段集合
                List<Field> fields = schema.fields();
                //遍历每个字段
                for (Field field : fields) {
                    //将每个字段的名字作为k,每个字段对应的值作为value
                    beforeJson.put(field.name(), structbefore.get(field));
                }
            }


            //5.获取after数据
            Struct structafter = value.getStruct("before");
            //new一个放before数据的json
            JSONObject afterJson = new JSONObject();
            if (structafter != null) {
                //因为每个数据的字段都不一样,所以用schema,遍历每个字段
                Schema schema = structafter.schema();
                //获取所有字段集合
                List<Field> fields = schema.fields();
                //遍历每个字段
                for (Field field : fields) {
                    //将每个字段的名字作为k,每个字段对应的值作为value
                    afterJson.put(field.name(), structafter.get(field));
                }
            }

            //将数据封装到JSONObject
            result.put("database", database);
            result.put("tableName", tableName);
            result.put("before", beforeJson);
            result.put("after", afterJson);
            result.put("type", type);

            //将数据发送到下游
            collector.collect(result.toJSONString());


        }

        @Override
        public TypeInformation<String> getProducedType() {
            //仿照DebeziumDeserializationSchema的其他实现类
            return BasicTypeInfo.STRING_TYPE_INFO;
        }
    }
}

