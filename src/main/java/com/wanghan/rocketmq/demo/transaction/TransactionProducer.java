package com.wanghan.rocketmq.demo.transaction;

import com.wanghan.rocketmq.constants.ApplicationConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * 事务消息
 * @Author WangHan
 * @Create 2020/3/9 1:38 上午
 */
@Slf4j
public class TransactionProducer {

    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        String[] tag = {"tag1", "tag2", "tag3"};
        TransactionMQProducer producer = new TransactionMQProducer("group1");
        producer.setNamesrvAddr(ApplicationConstants.NAME_SERVER_ADDR);
        //添加一个事务监听器
        producer.setTransactionListener(new TransactionListener() {
            /**
             * 执行本地事务
             */
            @Override
            public LocalTransactionState executeLocalTransaction(Message message, Object o) {
                if (tag[0].equals(message.getTags())){
                    return LocalTransactionState.COMMIT_MESSAGE;
                }else if (tag[1].equals(message.getTags())){
                    return LocalTransactionState.ROLLBACK_MESSAGE;
                }else {
                    //针对UNKNOW类型的事务状态会进去checkLocalTransaction进行回查
                    return LocalTransactionState.UNKNOW;
                }
            }

            /**
             * 回查本地事务
             */
            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {
                log.info("CheckLocalTransaction消息的tag:{}", messageExt.getTags());
                return LocalTransactionState.COMMIT_MESSAGE;
            }
        });
        producer.start();

        for (int i = 0; i < 3; i++) {
            //tag相当于针对topic体进行小的分类
            Message message = new Message(ApplicationConstants.TOPIC, tag[i], ("I send message:" + i).getBytes());
            //发送事务消息
            //arg传null表示针对所有的消息
            TransactionSendResult sendResult = producer.sendMessageInTransaction(message, null);
//            sendResult.getLocalTransactionState()

            log.info("SendStatus:{}",sendResult.getLocalTransactionState());
            Thread.sleep(1000);
        }
        //有序消息需要回查，所以就不要shutdown了
//        producer.shutdown();
    }
}
