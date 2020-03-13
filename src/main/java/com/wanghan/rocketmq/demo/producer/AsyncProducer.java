package com.wanghan.rocketmq.demo.producer;

import com.wanghan.rocketmq.constants.ApplicationConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * 发送异步消息
 *
 * @Author WangHan
 * @Create 2020/3/9 1:38 上午
 */
@Slf4j
public class AsyncProducer {

    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        DefaultMQProducer producer = new DefaultMQProducer("group1");
        producer.setNamesrvAddr(ApplicationConstants.NAME_SERVER_ADDR);
        producer.start();

        for (int i = 0; i < 10; i++) {
            //tag相当于针对topic体进行小的分类
            Message message = new Message(ApplicationConstants.TOPIC, ApplicationConstants.ASYNC_TAG, ("I send message:" + i).getBytes());
            producer.send(message, new SendCallback(){

                @Override
                public void onSuccess(SendResult sendResult) {
                    SendStatus sendStatus = sendResult.getSendStatus();
                    log.info("AsyncSendStatus:{}.MsgId:{}", sendStatus, sendResult.getMsgId());
                }

                @Override
                public void onException(Throwable throwable) {
                    log.info("AsyncSendError : {}", throwable.getMessage());
                }
            });

            Thread.sleep(1000);
        }

        producer.shutdown();
    }
}
