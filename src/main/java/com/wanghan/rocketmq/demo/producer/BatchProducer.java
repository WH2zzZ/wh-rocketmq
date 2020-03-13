package com.wanghan.rocketmq.demo.producer;

import com.wanghan.rocketmq.constants.ApplicationConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.ArrayList;
import java.util.List;

/**
 * 发送单向消息
 * 不关心发送结果的场景，比如日志
 *
 * @Author WangHan
 * @Create 2020/3/9 1:38 上午
 */
@Slf4j
public class BatchProducer {

    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        DefaultMQProducer producer = new DefaultMQProducer("group1");
        producer.setNamesrvAddr(ApplicationConstants.NAME_SERVER_ADDR);
        producer.start();


        String topic = "BatchTest";
        List<Message> messages = new ArrayList<>();
        messages.add(new Message(ApplicationConstants.TOPIC, ApplicationConstants.BATCH_TAG, ("I send batch message:" + 1).getBytes()));
        messages.add(new Message(ApplicationConstants.TOPIC, ApplicationConstants.BATCH_TAG, ("I send batch message:" + 2).getBytes()));
        messages.add(new Message(ApplicationConstants.TOPIC, ApplicationConstants.BATCH_TAG, ("I send batch message:" + 3).getBytes()));
        try {
            producer.send(messages);
        } catch (Exception e) {
            e.printStackTrace();
            //handle the error
        }finally {
            producer.shutdown();
        }

    }
}
