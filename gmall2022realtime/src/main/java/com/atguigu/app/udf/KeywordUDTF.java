package com.atguigu.app.udf;

import com.atguigu.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.List;

/**
 * @ClassName KeywordUDTF
 * @Description TODO
 * @Author 10564
 * @Date 2022/2/19 0:26
 * @Version 1.0
 */
//desc:自定义udtf函数实现分词功能

// @FunctionHint 主要是为了标识输出数据的类型
// row.setField(0,keyword)中的0表示返回值下标为0的值
@FunctionHint(output = @DataTypeHint("ROW<keyword STRING>"))
public class KeywordUDTF extends TableFunction<Row> {
    public void eval(String value) {

        List<String> keywordList = KeywordUtil.analyze(value);
        for (String keyword : keywordList) {
            collect(Row.of(keyword));

        }
    }
}

