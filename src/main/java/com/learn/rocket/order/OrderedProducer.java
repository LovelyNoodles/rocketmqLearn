package com.learn.rocket.order;

import java.util.List;

import com.alibaba.rocketmq.client.ClientConfig;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.MQProducer;
import com.alibaba.rocketmq.client.producer.MessageQueueSelector;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;

public class OrderedProducer {
	public static void main(String[] args) throws Exception {
		// Instantiate with a producer group name.
		MQProducer producer = new DefaultMQProducer("producerGroupTest");
		((ClientConfig) producer).setNamesrvAddr("127.0.0.1:9876");
		// Launch the instance.
		producer.start();
		String[] tags = new String[] { "TagA", "TagB", "TagC", "TagD", "TagE" };
		for (int i = 0; i < 10; i++) {
			int orderId = i % 10;
			// Create a message instance, specifying topic, tag and message body.
			Message msg = new Message("TopicTestjjj", tags[i % tags.length], "KEY" + i,
					("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
			SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
				public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
					Integer id = (Integer) arg;
					int index = id % mqs.size();
					return mqs.get(index);
				}
			}, orderId);

			System.out.printf("%s%n", sendResult);
		}
		// server shutdown
		producer.shutdown();
	}
}
