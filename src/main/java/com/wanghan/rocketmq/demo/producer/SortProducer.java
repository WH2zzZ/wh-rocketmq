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
public class SortProducer {

    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        DefaultMQProducer producer = new DefaultMQProducer("group1");
        producer.setNamesrvAddr(ApplicationConstants.NAME_SERVER_ADDR);
        producer.start();
        for (int i = 0; i < 5; i++) {
            for (int j = 0; j < 10; j++){

                final int messageType = i;
                //tag相当于针对topic体进行小的分类
                /**
                 * 参数一：消息体
                 * 参数二：消息队列的选择器
                 * 参数三：选择的消息体排序标识，保证某一类型的消息发送是有序的
                 */
                Message message = new Message(ApplicationConstants.TOPIC, ApplicationConstants.SORT_TAG, ("I send message:" + j).getBytes());
                producer.send(message, new MessageQueueSelector() {
                    @Override
                    public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                        //通过取模来保证同一类型数据进入同一个队列
                        int queueCode = messageType % list.size();
                        return list.get(queueCode);
                    }
                }, messageType);

                log.info("SendStatus-success");
                Thread.sleep(1000);
            }
        }
        producer.shutdown();
    }
}
