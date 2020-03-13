package com.wanghan.rocketmq.demo.consumer;

import com.wanghan.rocketmq.constants.ApplicationConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;
import java.util.stream.Collectors;

/**
 * 消费者
 * @Author WangHan
 * @Create 2020/3/10 1:57 上午
 */
@Slf4j
public class SortConsumer {

    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("group1");
        consumer.setNamesrvAddr(ApplicationConstants.NAME_SERVER_ADDR);


        consumer.subscribe(ApplicationConstants.TOPIC, ApplicationConstants.SORT_TAG);

        //通过MessageListenerOrderly来保证顺序消费
        consumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> list, ConsumeOrderlyContext consumeOrderlyContext) {
                List<String> messages = list.stream().map(messageExt -> {
                    return new String(messageExt.getBody());
                }).collect(Collectors.toList());
                //这里可以明显的看到不同的线程消费不同的broker内部的队列
                log.info("Thread:{},Message:{}", Thread.currentThread().getName(), messages);
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });

        consumer.start();
    }
}
