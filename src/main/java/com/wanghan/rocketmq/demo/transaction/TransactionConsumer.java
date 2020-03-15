package com.wanghan.rocketmq.demo.transaction;

import com.wanghan.rocketmq.constants.ApplicationConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;
import java.util.stream.Collectors;

/**
 * 通用消费者
 * @Author WangHan
 * @Create 2020/3/10 1:57 上午
 */
@Slf4j
public class TransactionConsumer {

    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("group1");
        consumer.setNamesrvAddr(ApplicationConstants.NAME_SERVER_ADDR);

        //tag类型可以 tag1 || tag2，订阅多个tag的消息，如果想订阅所有tag就用*
        consumer.subscribe(ApplicationConstants.TOPIC, "*");

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                List<String> messages = msgs.stream().map(messageExt -> {
                    return new String(messageExt.getBody());
                }).collect(Collectors.toList());
                log.info("MessageBody:{}", messages);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.start();
    }
}
