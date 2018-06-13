package com.learn.rocket;

import com.alibaba.rocketmq.common.MixAll;

/**
 * Hello world!
 *
 */
public class App {
	public static void main(String[] args) {
		System.out.println(System.getenv(MixAll.NAMESRV_ADDR_ENV));
	}
}
