package com.zhuweihao.Utils;

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
 * @Author zhuweihao
 * @Date 2022/12/5 17:55
 * @Description com.zhuweihao.Utils
 */
public class CustomerJsonDeserializationSchema implements DebeziumDeserializationSchema<String> {
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector collector) throws Exception {
        //创建JSON对象用于封装结果
        JSONObject result = new JSONObject();
        //获取before数据
        sourceRecord.valueSchema();
        Struct value = (Struct) sourceRecord.value();
        Struct before = value.getStruct("before");
        JSONObject beforejson = new JSONObject();
        if (before != null) {
            Schema schema = before.schema();
            List<Field> fields = schema.fields();
            for (Field field :
                    fields) {
                beforejson.put(field.name(), before.get(field));
            }
        }
        result.put("before", beforejson);
        //获取after数据
        sourceRecord.valueSchema();
        Struct after = value.getStruct("after");
        JSONObject afterjson = new JSONObject();
        if (after != null) {
            Schema schema = after.schema();
            List<Field> fields = schema.fields();
            for (Field field :
                    fields) {
                afterjson.put(field.name(), after.get(field));
            }
        }
        result.put("after", afterjson);
        //获取操作类型
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        result.put("op", operation);
        //输出结果
        collector.collect(result.toJSONString());
    }

    @Override
    public TypeInformation getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
