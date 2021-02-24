package com.intsmaze.flink.streaming.connector.source;

import com.google.gson.Gson;
import com.intsmaze.flink.streaming.bean.SchemaBean;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

/**
 * github地址: https://github.com/ChiYaoLa
 * 
 * 参阅 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
 *
 * @auther: xuliang
 * @date: 2020/10/15 18:33
 */
public class KafkaDeserializationSchema extends AbstractDeserializationSchema<SchemaBean> {

    /**
     * github地址: https://github.com/ChiYaoLa
     * 
     * 参阅 出版书籍《深入理解Flink核心设计与实践原理》 随书代码
     *
     * @auther: xuliang
     * @date: 2020/10/15 18:33
     */
    @Override
    public SchemaBean deserialize(byte[] message) {
        Gson gson = new Gson();
        return gson.fromJson(new String(message), SchemaBean.class);
    }

}
