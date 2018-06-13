package com.learn.rocket.batch;

import java.util.List;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;

public class BatchConsumer {
	public static void main(String[] args) throws Exception {
		DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("BatchConsumerGroup");

		consumer.setNamesrvAddr("127.0.0.1:9876");
		
		consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

		consumer.subscribe("TopicTest", "TagA || TagC || TagD");

		consumer.registerMessageListener(new MessageListenerConcurrently() {

			public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
				System.out.printf(Thread.currentThread().getName() + " Receive New Messages: " + msgs + "%n");
				return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
			}
		});

		consumer.start();
		System.out.printf("Broadcast Consumer Started.%n");
	}
}
