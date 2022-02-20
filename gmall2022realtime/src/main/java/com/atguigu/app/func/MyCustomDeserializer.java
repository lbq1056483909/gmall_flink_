package com.atguigu.app.func;
/**
 * @ClassName FlinkCDC_DataStream_Deserializer
 * @Description TODO
 * @Author 10564
 * @Date 2022/1/24 13:21
 * @Version 1.0
 */

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
//自定义反序列化器,来处理flinkcdc传到kafka中的格式
import java.util.List;


public class MyCustomDeserializer implements DebeziumDeserializationSchema<String> {
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        //1.创建一个JSONObject用来存放最终封装好的数据
        JSONObject result = new JSONObject();

        //2.获取数据库以及表名
        String topic = sourceRecord.topic();
        String[] split = topic.split("\\.");

        //数据库名
        String database = split[1];
        //表名
        String tableName = split[2];

        //3.获取类型
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        //将获取到的类型转为小写字母
        String type = operation.toString().toLowerCase();
        //将create替换成insert
        if ("create".equals(type) || "read".equals(type)){
            type = "insert";
        }

        //4.获取数据,将sourceRecord转换为Struct格式
        Struct value = (Struct) sourceRecord.value();

        //5.获取before数据(before里面还是一个json)
        Struct structBefore = value.getStruct("before");
        //重新创建一个json对象,放before里面的数据
        JSONObject beforeJson = new JSONObject();
        if (structBefore!=null){
            Schema schema = structBefore.schema();
            List<Field> fields = schema.fields();
            for (Field field : fields) {
                beforeJson.put(field.name(), structBefore.get(field));
            }
        }

        //6.获取after数据
        Struct structAfter = value.getStruct("after");
        JSONObject afterJson = new JSONObject();
        if (structAfter!=null){
            Schema schema = structAfter.schema();
            List<Field> fields = schema.fields();
            for (Field field : fields) {
                afterJson.put(field.name(), structAfter.get(field));
            }
        }

        //将数据封装到JSONObject中
        result.put("database", database);
        result.put("tableName", tableName);
        result.put("before", beforeJson);
        result.put("after", afterJson);
        result.put("type", type);

        //将数据发送至下游,下游是kafka,所以要将json对象转换为json字符串
        collector.collect(result.toJSONString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}

