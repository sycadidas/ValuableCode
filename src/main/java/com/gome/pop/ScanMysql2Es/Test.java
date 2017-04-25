package com.gome.pop.ScanMysql2Es;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.elasticsearch.client.Client;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;


public class Test {
	
	
	
	public static void main(String[] args) throws Exception{
		
		 ApplicationContext context = new ClassPathXmlApplicationContext("applicationContext.xml");
		 
		 Client client =null;
		 try {
				client = TransportClient.builder().build()  
			            .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.58.70.87"), 9300));//10.58.62.47 杨宇集群
				
			} catch (Exception e) {
				e.printStackTrace();
			}
		 
		int count = 20000; //总数
		int page_size=1000; //每次10w条
		int pages = count / page_size + 1; // 一共开多少个线程
		
		System.out.println("一共开启了"+pages+"个线程");
		
		ExecutorService service =Executors.newFixedThreadPool(4);
		
		List<Runner> list=new ArrayList<Runner>();                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  
		
		for(int i=1;i<=pages;i++){
			Runner runner=new Runner(client,context,"线程"+i,page_size,i,(i-1)*page_size);
			list.add(runner);
		}
		
		service.invokeAll(list);
		service.shutdown();
	}
	
	
	static class Runner implements Callable<Long>{
		
		private String threadName;
		private int page_size; //当前线程一次查询多少条记录
		private int page;  //从第几页开始查询
		private int start_index;//从第几条记录开始查询
		private ApplicationContext context;
		private Client client;
		
		public Runner(Client client,ApplicationContext context,String threadName,int page_size,int page,int start_index){
			this.context = context;
			this.page_size=page_size;
			this.page=page;
			this.threadName=threadName;
			this.start_index=start_index;
			this.client = client;
		}
		
		public Long call() {
			long startTime=new Date().getTime();
			try {
				//通过分页查询数据库
				Start.creatJsonFromDB(start_index,page_size,context,client);
				
				System.out.println(threadName+"正处理从"+(page - 1) * page_size+"条开始的数据=============cost了"+(new Date().getTime()-startTime)/1000+"秒");
			} catch (Exception e) {
				e.printStackTrace();
			}
			return Long.valueOf(new Date().getTime()-startTime);
		}
	}
}
