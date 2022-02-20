package com.test2.app.func;

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

import java.util.List;

/**
 * @ClassName MyCustomDeserializer
 * @Description TODO
 * @Author 10564
 * @Date 2022/2/10 17:08
 * @Version 1.0
 */
//这个类的目的是自定义反序列化器,将flinkcdc从mysql读过来的数据(已经序列化了),所以需要反序列化处理一下
//并且反序列化转为我们方便处理的格式
public class MyCustomDeserializer implements DebeziumDeserializationSchema<String> {
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        //1.创建一个JSONObject用来存放最终分装好的数据
        JSONObject result = new JSONObject();
        //SourceRecord{sourcePartition={server=mysql_binlog_source},
        //             sourceOffset={ts_sec=1644483570, file=mysql-bin.000034, pos=15326, snapshot=true}}
        //ConnectRecord{topic='mysql_binlog_source.gmall_flink.user_info',
        //              kafkaPartition=null,
        //              key=Struct{id=3920},
        //              keySchema=Schema{mysql_binlog_source.gmall_flink.user_info.Key:STRUCT},
        //              value=Struct{after=Struct{id=3920,login_name=5v7j3p5,nick_name=春菊,name=熊春菊,phone_num=13396964726,email=5v7j3p5@sohu.com,user_level=3,birthday=1978-12-04,gender=F,create_time=2020-12-04 23:28:45},source=Struct{version=1.5.2.Final,connector=mysql,name=mysql_binlog_source,ts_ms=1644483570050,snapshot=true,db=gmall_flink,table=user_info,server_id=0,file=mysql-bin.000034,pos=15326,row=0},op=r,ts_ms=1644483570050}, valueSchema=Schema{mysql_binlog_source.gmall_flink.user_info.Envelope:STRUCT}, timestamp=null, headers=ConnectHeaders(headers=)}
        //2.获取数据库以及表名
        //底层看出:SourceRecord extends ConnectRecord
        //根据java的继承,子类可以继承父类的方法
        //topic='mysql_binlog_source .  gmall_flink   .  user_info
        String[] split = sourceRecord.topic().split("\\.");
        String database =  split[1];
//        System.out.println(database);
        String tableName = split[2];
//        System.out.println(tableName);

        //3.获取类型(这个获取操作类型比如insert,updata,delete)
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        //将操作类型转换为小写字母
        String type = operation.toString().toLowerCase();
        //将create替换成insert
        if ("create".equals(type) || "read".equals(type)) {
            type = "insert";
        }
        //4.获取数据,将sourceRecord转换为struct格式(sourceRecord继承于ConnectRecord,所以直接调用value就行)
        Struct value = (Struct) sourceRecord.value();
        //5.获取before数据(before里面还是一个json,)
        Struct structbefore = value.getStruct("before");
        //重新创建一个json对象,放before里面的数据
        JSONObject beforejsonObject = new JSONObject();
        if (structbefore != null) {
            //获取before这个struct里面的schema
            Schema schema = structbefore.schema();
            List<Field> fields = schema.fields();
            //获取里面每一个字段,然后通过字段获取每一个字段的value
            for (Field field : fields) {
                //直接将json当作map来用就行,也就是kv类型
//                System.out.println("des");
//                System.out.println("field"+field);
                beforejsonObject.put(field.name(), structbefore.get(field));
            }

        }
        //6.获取after数据
        Struct structafter = value.getStruct("after");
        //重新创建一个json对象,放before里面的数据
        JSONObject afterjsonObject = new JSONObject();
        if (structafter != null) {
            //获取before这个struct里面的schema
            Schema schema = structafter.schema();
            List<Field> fields = schema.fields();
            //获取里面每一个字段,然后通过字段获取每一个字段的value
            for (Field field : fields) {
                //直接将json当作map来用就行,也就是kv类型
                //System.out.println(field);//Field{name=status, index=13, schema=Schema{STRING}}
                afterjsonObject.put(field.name(), structafter.get(field));
            }
        }

        //6.将数据封装到JSONObject(json可以当成map处理,也可以用put,get)
        result.put("database",database);//数据库
        result.put("tableName",tableName);//表名
        result.put("before",beforejsonObject);//修改前数据
        result.put("after",afterjsonObject);
        result.put("type",type);

        //将数据发送至下游,下游是kafka,将数据转换为json字符串
        collector.collect(result.toJSONString());

        //


    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}

