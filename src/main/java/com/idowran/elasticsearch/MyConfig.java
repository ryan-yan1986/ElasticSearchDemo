package com.idowran.elasticsearch;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MyConfig {
	
	@Bean
	public TransportClient client() throws UnknownHostException {
		// 配置ElasticSearch的客户端
		TransportAddress node = new TransportAddress(
				InetAddress.getByName("localhost"),
				9300);
		
		// 集群名不配置会采取默认的
		Settings settings = Settings.builder()
				.put("cluster.name", "tony")
				.build();
		// 不需要自定义配置，而已写入空配置Settings.EMPTY
		TransportClient client = new PreBuiltTransportClient(settings);
		client.addTransportAddress(node);
		return client;
	}
}
