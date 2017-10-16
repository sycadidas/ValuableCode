package com.gome.pop.ScanMysql2Es.es.elesticsearch;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.log4j.Logger;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.springframework.beans.factory.InitializingBean;

import com.founder.bbc.diamond.DiamondOP;


/**
 * 
* @ClassName: ElasticSearchClientConfig 
* @Description:esearch客户端初始化 
* @author sunyanchen 
* @date 2017年3月14日 上午11:01:37
 */
public class ElasticSearchClientConfig  implements InitializingBean{
	
	private static  Logger log = Logger.getLogger(ElasticSearchClientConfig.class);
	
	private static int DEFAULT_PORT = Integer.parseInt(DiamondOP.getProperty("POP_ORDER", "ORDER_ES_CONF", "es_port", ""));
	
	private static String hosts = DiamondOP.getProperty("POP_ORDER", "ORDER_ES_CONF", "es_hosts", "");
	
	private static String clusterName = DiamondOP.getProperty("POP_ORDER", "ORDER_ES_CONF", "es_clusterName", "");
	 
	public  static TransportClient client =null;
	
	
	public void afterPropertiesSet() throws Exception {
		String[] hostNames = hosts.trim().split(",");
		
		InetSocketTransportAddress[] serverAddresses = new InetSocketTransportAddress[hostNames.length];
		
		for (int i = 0; i < hostNames.length; i++) {
			String[] hostPort = hostNames[i].trim().split(":");
			String host = hostPort[0];
			int port = hostPort.length == 2 ? Integer.parseInt(hostPort[1]) : DEFAULT_PORT;
			try {
				serverAddresses[i] = new InetSocketTransportAddress(InetAddress.getByName(host), port);
			} catch (UnknownHostException e) {
				log.error(e.getLocalizedMessage());
			}
		}
		
		Settings settings = Settings.settingsBuilder()
				.put("cluster.name", clusterName)
				.put("ignore_unavailable",true)
				.build();
		
		 client = TransportClient.builder()
				.settings(settings)
				.build();
		for (InetSocketTransportAddress host : serverAddresses) {
			client.addTransportAddress(host);
		}
		
	}
	
	
}
