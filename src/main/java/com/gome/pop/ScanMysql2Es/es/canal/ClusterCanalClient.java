package com.gome.pop.ScanMysql2Es.es.canal;

import java.net.InetSocketAddress;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.springframework.beans.factory.InitializingBean;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.founder.bbc.diamond.DiamondOP;

public class ClusterCanalClient extends AbstractCanalClient implements InitializingBean{
	
	public ClusterCanalClient() {
		// TODO Auto-generated constructor stub
	}
	
	public ClusterCanalClient(String destination) {
		super(destination);
	}

	public void afterPropertiesSet() throws Exception {
		
		Class.forName("oracle.jdbc.driver.OracleDriver");   
		
		String destination = DiamondOP.getProperty("POP_ORDER", "ORDER_ES_CONF", "canal_destination", "");
		logger.error(destination);
		// 基于zookeeper动态获取canal server的地址，建立链接，其中一台server发生crash，可以支持failover
		CanalConnector connector = CanalConnectors.newClusterConnector(
				DiamondOP.getProperty("POP_ORDER", "ORDER_ES_CONF", "canal_zookeeper", ""), destination, "", "");

//		CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress("10.58.57.71",
//	            11111), "example", "", "");
//		
		final ClusterCanalClient clientTest = new ClusterCanalClient(
				destination);
		  
		clientTest.setConnector(connector);
		clientTest.start();

		Runtime.getRuntime().addShutdownHook(new Thread() {

			public void run() {
				try {
					logger.error("## stop the canal client");
					clientTest.stop();
				} catch (Throwable e) {
					logger.error("##something goes wrong when stopping canal:\n{}"+ExceptionUtils.getFullStackTrace(e));
				} finally {
					logger.error("## canal client is down.");
				}
			}

		});
	}
	
}
