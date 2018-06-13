package com.learn.rocket.batch;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.common.message.Message;

public class BatchProducer {
	public static void main(String[] args) throws Exception {
		DefaultMQProducer producer = new DefaultMQProducer("BatchProducerGroup");
		producer.setNamesrvAddr("127.0.0.1:9876");
		producer.start();

		String topic = "BatchTest";
		List<Message> messages = new ArrayList<Message>();
		messages.add(new Message(topic, "TagA", "OrderID001", "Hello world 0".getBytes()));
		messages.add(new Message(topic, "TagA", "OrderID002", "Hello world 1".getBytes()));
		messages.add(new Message(topic, "TagA", "OrderID003", "Hello world 2".getBytes()));
		try {

			// then you could split the large list into small ones:
			ListSplitter splitter = new ListSplitter(messages);
			while (splitter.hasNext())
				try {
					List<Message> listItem = splitter.next();
					// TODO send messages
//					producer.send(listItem);
				} catch (Exception e) {
					e.printStackTrace();
					// handle the error
				}
		} catch (Exception e) {
			e.printStackTrace();
			// handle the error
		}
		// System.out.printf("%s%n", sendResult);
		producer.shutdown();
	}
}
