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
 * 延迟消费者
 * @Author WangHan
 * @Create 2020/3/10 1:57 上午
 */
@Slf4j
public class DelayConsumer {

    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("group1");
        consumer.setNamesrvAddr(ApplicationConstants.NAME_SERVER_ADDR);

        consumer.subscribe(ApplicationConstants.TOPIC, ApplicationConstants.DELAY_TAG);

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                msgs.forEach(messageExt -> {
                    String msg = new String(messageExt.getBody());
                    //获取到延迟的时间
                    log.info("Message:{}, delayTime:{}", msg, System.currentTimeMillis() - messageExt.getBornTimestamp());

                });
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.start();
    }
}
