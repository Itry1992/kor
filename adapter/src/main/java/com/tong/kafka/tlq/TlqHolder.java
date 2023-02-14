package com.tong.kafka.tlq;

import com.tongtech.client.common.ModeType;
import com.tongtech.client.consumer.impl.TLQPullConsumer;
import com.tongtech.client.exception.TLQClientException;
import com.tongtech.client.producer.TLQProducer;

public class TlqHolder {
    private TlqHolder() {
    }

    public static String topic = "topic1_kafka";

    public static TLQProducer getProducer() {
        return Holder.Instance.tlqProducer;
    }

    public static TLQPullConsumer getCustomer() {
        return Holder.Instance.consumer;
    }

    private enum Holder {
        Instance;
        private final TLQProducer tlqProducer;
        private final TLQPullConsumer consumer;
        private final String domainName = "domain1";
        private final String nameSrv = "tcp://192.168.50.2:9888";

        Holder() {
            tlqProducer = new TLQProducer();
            consumer = new TLQPullConsumer();
            try {
                tlqProducer.setModeType(ModeType.TOPIC);
                tlqProducer.setNamesrvAddr("tcp://192.168.50.2:9888");
                tlqProducer.setDomain(domainName);
                tlqProducer.start();
                consumer.setNamesrvAddr(nameSrv);                                             /* 管理节点地址和端口号，tcp[udp]://地址：端口  例如tcp://xxxxxxx:xxxx   */
                consumer.subscribe(topic);                                             /* 订阅主题 */
                consumer.setDomain(domainName);                                               /* 通信域名称,仅支持为英文字母、阿拉伯数字、特殊字符(下划线 点)的任意组合，首字符必需是英文字母，最大长度64字节 */
                consumer.setModeType(ModeType.TOPIC);
                consumer.start();
            } catch (TLQClientException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
