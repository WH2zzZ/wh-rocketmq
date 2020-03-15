package com.wanghan.rocketmq.demo.consumer;

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
public class Consumer {

    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("group1");
        consumer.setNamesrvAddr(ApplicationConstants.NAME_SERVER_ADDR);

        //设定消费模式：负载均衡|广播模式
        //默认是负载均衡
        consumer.setMessageModel(MessageModel.BROADCASTING);

        //tag类型可以 tag1 || tag2，订阅多个tag的消息，如果想订阅所有tag就用*
        consumer.subscribe(ApplicationConstants.TOPIC, ApplicationConstants.SORT_TAG);

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                List<String> messages = msgs.stream().map(messageExt -> {
                    return new String(messageExt.getBody());
                }).collect(Collectors.toList());
                log.info("Message:{}", msgs);
                log.info("MessageBody:{}", messages);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                //消息将重试
//                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
//                return null;
//                throw new Exception();
            }
        });

        consumer.start();
    }
}
