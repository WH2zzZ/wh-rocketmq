package com.wanghan.rocketmq.demo.producer;

import com.wanghan.rocketmq.constants.ApplicationConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.List;

/**
 * 顺序发送消息
 * 不关心发送结果的场景，比如日志
 *
 * @Author WangHan
 * @Create 2020/3/9 1:38 上午
 */
@Slf4j
public class DelayProducer {

    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        DefaultMQProducer producer = new DefaultMQProducer("group1");
        producer.setNamesrvAddr(ApplicationConstants.NAME_SERVER_ADDR);
        producer.start();

        for (int i = 0; i < 10; i++) {
            //tag相当于针对topic体进行小的分类
            Message message = new Message(ApplicationConstants.TOPIC, ApplicationConstants.DELAY_TAG, ("I send delay message:" + i).getBytes());
            //1s  5s  10s  30s  1m  2m  3m  4m  5m  6m  7m  8m  9m  10m  20m  30m  1h  2h
            message.setDelayTimeLevel(4);

            producer.send(message);

            log.info("SendStatus-success");
            Thread.sleep(1000);
        }

        producer.shutdown();
    }
}
