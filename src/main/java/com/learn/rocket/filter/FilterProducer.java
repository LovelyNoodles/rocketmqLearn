package com.learn.rocket.filter;

import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;

public class FilterProducer {
	public static void main(String[] args) throws Exception {
		DefaultMQProducer producer = new DefaultMQProducer("FilterProducerGroup");
		producer.setNamesrvAddr("127.0.0.1:9876");
		producer.start();

		for (int i = 0; i < 100; i++) {
			Message msg = new Message("TopicTest", ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
			// Set some properties.
			msg.putUserProperty("a", String.valueOf(i));

			SendResult sendResult = producer.send(msg);
			System.out.printf("%s%n", sendResult);
		}
		producer.shutdown();
	}
}
