package com.gome.pop.ScanMysql2Es;

import java.net.InetAddress;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import com.alibaba.fastjson.JSONObject;
import com.gome.pop.ScanMysql2Es.base.BaseDAO;

/**
 * Hello world!
 *
 */
public class Start 
{

	static Integer countSql() throws SQLException{
		String countSql = "SELECT count(so.SUB_ORDER_ID) as totalCount"
		 		+ " FROM tbl_sub_order so  LEFT JOIN tbl_order_consignee oc ON so.sub_Order_Id = oc.sub_order_id  "
		 		+ " LEFT JOIN tbl_tracking_no tn ON so.sub_Order_Id = tn.sub_order_id "
		 		+ " LEFT JOIN (SELECT s.SUB_ORDER_ID, GROUP_CONCAT( DISTINCT s.sku) SKU,GROUP_CONCAT(DISTINCT  s.GOODS_NAME) GOODS_NAME  from tbl_cart_goods s GROUP BY s.SUB_ORDER_ID) aa on so.sub_Order_Id = aa.SUB_ORDER_ID";
		
		  @SuppressWarnings("resource")
		  ApplicationContext context = new ClassPathXmlApplicationContext("applicationContext.xml");
	      BaseDAO dao=(BaseDAO)context.getBean("baseDAO");
	      Connection conn = dao.getConnection();
	      PreparedStatement stmt = conn.prepareStatement(countSql);
	      ResultSet set=stmt.executeQuery();
	      
	      if(set.next()){
	    	  return set.getInt("totalCount");
	      }else{
	    	  return null;
	      }
	      
//	      return set.getInt("totalCount");
	      
		
	}
	
	static void creatJsonFromDB(int startIndex, int page_size) throws Exception{
		 String sql="SELECT so.ORDER_GOODS_TYPE ORDER_GOODS_TYPE, date_format(so.DLRT_TIME, '%Y-%m-%d %h:%i:%s') DLRT_TIME, so.SALE_TAX SALE_TAX,so.VAT_TAX VAT_TAX,so.COMPREHENSIVE_TAX COMPREHENSIVE_TAX,"
		 		+ "so.order_no ORDER_NO,so.sub_Order_Id SUB_ORDER_ID,so.order_Origin ORDER_ORIGIN,so.SUB_ORDER_TYPE SUB_ORDER_TYPE,so.total_Price TOTAL_PRICE,date_format(so.order_Date, '%Y-%m-%d %h:%i:%s') ORDER_DATE,"
		 		+ "so.order_State ORDER_STATE,	date_format(so.order_State_Time, '%Y-%m-%d %h:%i:%s') ORDER_STATE_TIME,so.REMARK,so.opinion_desc OPINION_DESC,so.pay_model PAY_MODEL,so.FREIGHT,so.pay_sate PAY_SATE,so.own_shop OWN_SHOP,"
		 		+ "so.gome_tracking GOME_TRACKING,so.shop_no SHOP_NO,so.INVOICE_TYPE INVOICE_TYPE,so.part_discount_price  PART_DISCOUNT_PRICE,so.coupon_value  COUPON_VALUE,so.shop_name SHOP_NAME,"
		 		+ "so.custmer_tax_num CUSTMER_TAX_NUM,so.gome_freight GOME_FREIGHT,so.is_invoiced IS_INVOICED,so.head_type HEAD_TYPE,so.warehouse_name WAREHOUSE_NAME,so.warehouse_id WAREHOUSE_ID,"
		 		+ "	date_format(so.receive_time, '%Y-%m-%d %h:%i:%s') RECEIVE_TIME,  date_format(so.two_delivery_time, '%Y-%m-%d %h:%i:%s') TWO_DELIVERY_TIME,so.two_delivery_desc TWO_DELIVERY_DESC,so.two_delivery_flag TWO_DELIVERY_FLAG,so.receivable_amount RECEIVABLE_AMOUNT,"
		 		+ "so.abflag  ABFLAG,so.PRE_PAYMENT  PRE_PAYMENT,date_format(so.ex_time, '%Y-%m-%d %h:%i:%s') EX_TIME,so.BLACKLIST_ID  BLACKLIST_ID,so.BLACKLIST_TYPE  BLACKLIST_TYPE,so.status_reason_desc  STATUS_REASON_DESC,"
		 		+ "so.REASON  REASON,so.NO_EX_REASON  NO_EX_REASON, date_format(so.EXPECT_EX_DATE, '%Y-%m-%d %h:%i:%s') EXPECT_EX_DATE,so.COD_TERM  COD_TERM,so.shop_flag  SHOP_FLAG,so.total_postal_tax  TOTAL_POSTAL_TAX,so.id_name  ID_NAME,"
		 		+ "so.id_num  ID_NUM, date_format(so.TAKED_EXPRESS_TIME, '%Y-%m-%d %h:%i:%s') TAKED_EXPRESS_TIME, date_format(so.SIGNED_TIME, '%Y-%m-%d %h:%i:%s') SIGNED_TIME,so.APPRAISE_FLAG  APPRAISE_FLAG,so.REMARK_LEVEL  REMARK_LEVEL,so.operator_desc OPERATOR_DESC,"
		 		+ "so.Revert2  PAYMENT_DISCOUNT, oc.zip_code ZIP_CODE ,oc.consignee CONSIGNEE,oc.mobile MOBILE,oc.phone PHONE,oc.province PROVINCE,oc.city CITY,oc.district DISTRICT,"
		 		+ "oc.state_name STATE_NAME,oc.address ADDRESS,oc.email EMAIL,date_format(oc.delivery_date, '%Y-%m-%d %h:%i:%s') DELIVERY_DATE,aa.SKU ,aa.GOODS_NAME, tn.TRACKING_NO   "
		 		+ " FROM tbl_sub_order so  LEFT JOIN tbl_order_consignee oc ON so.sub_Order_Id = oc.sub_order_id  "
		 		+ " LEFT JOIN tbl_tracking_no tn ON so.sub_Order_Id = tn.sub_order_id "
		 		+ " LEFT JOIN (SELECT s.SUB_ORDER_ID, GROUP_CONCAT( DISTINCT s.sku) SKU,GROUP_CONCAT(DISTINCT  s.GOODS_NAME) GOODS_NAME  from tbl_cart_goods s GROUP BY s.SUB_ORDER_ID) aa on so.sub_Order_Id = aa.SUB_ORDER_ID"
		 		+ " limit "+ startIndex+","+page_size;
		
		  @SuppressWarnings("resource")
		  ApplicationContext context = new ClassPathXmlApplicationContext("applicationContext.xml");
	      BaseDAO dao=(BaseDAO)context.getBean("baseDAO");
	      Connection conn = dao.getConnection();
	      PreparedStatement stmt = conn.prepareStatement(sql);
	      ResultSet set=stmt.executeQuery();
	      
	      // 获取列数  
	      ResultSetMetaData md = set.getMetaData();
	      int columnCount = md.getColumnCount();  
	      
	  	@SuppressWarnings("unused")
		Settings settings =  Settings.settingsBuilder().put("cluster.name", "elasticsearch").build();
		Client client =null;
		
		try {
			client = TransportClient.builder().build()  
		            .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.58.62.47"), 9300));
			//10.58.62.47 杨宇集群
		} catch (Exception e) {
			e.printStackTrace();
		}
		BulkRequestBuilder bulkRequest = client.prepareBulk();
		 
		int ss=0;
		
		
	      while(set.next()){
	    	  ss++;
	    	  JSONObject jsonObj = new JSONObject();  
	    	   // 遍历每一列  
	          for (int i = 1; i <= columnCount; i++) {  
	              String columnName =md.getColumnLabel(i);  
	              String value = set.getString(columnName);  
	              
	              if(columnName.equals("SKU")||columnName.equals("GOODS_NAME")){
	            	  if(value!=null){
	            		  String[] a=value.split(",");
			              jsonObj.put(columnName, a);  
	            	  }
		          }
	              else  {
	            	  jsonObj.put(columnName, value);
	              }
	          }   
	          bulkRequest.add(client.prepareIndex("sub_order_test",
						"tbl_sub_order").setId(set.getString("SUB_ORDER_ID")).setSource(jsonObj.toString()));
	      }
	      System.out.println("线程"+Thread.currentThread().getName()+"处理了"+ss+"条数据");
	      BulkResponse bulkResponse = bulkRequest.execute().actionGet();
	  	 
		if (bulkResponse.hasFailures()) {
			System.out.println(bulkResponse.buildFailureMessage());
		}
	  	  client.close();	
	    
	      set.close();
	      stmt.close();
	      conn.close();
	}
	
	
	
}
