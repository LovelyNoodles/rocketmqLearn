package com.learn.rocket.order;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerOrderly;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;

public class OrderedConsumer {
	public static void main(String[] args) throws Exception {
		DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("consumerGroupTest");
		
		consumer.setNamesrvAddr("127.0.0.1:9876");

		consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

		consumer.subscribe("TopicTestjjj", "TagA || TagC || TagD");

		consumer.registerMessageListener(new MessageListenerOrderly() {

			AtomicLong consumeTimes = new AtomicLong(0);

			public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
				context.setAutoCommit(false);
				System.out.printf(Thread.currentThread().getName() + " Receive New Messages: " + msgs + "%n");
				this.consumeTimes.incrementAndGet();
				if ((this.consumeTimes.get() % 2) == 0) {
					return ConsumeOrderlyStatus.SUCCESS;
				} else if ((this.consumeTimes.get() % 3) == 0) {
					return ConsumeOrderlyStatus.ROLLBACK;
				} else if ((this.consumeTimes.get() % 4) == 0) {
					return ConsumeOrderlyStatus.COMMIT;
				} else if ((this.consumeTimes.get() % 5) == 0) {
					return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
				}
				System.out.println(ConsumeOrderlyStatus.SUCCESS);
				return ConsumeOrderlyStatus.SUCCESS;

			}
		});

		consumer.start();

		System.out.printf("Consumer Started.%n");
	}
}
