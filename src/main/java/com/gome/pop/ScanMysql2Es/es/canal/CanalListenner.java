package com.gome.pop.ScanMysql2Es.es.canal;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;





import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.founder.bbc.diamond.DiamondOP;
import com.gome.pop.dao.OrderDao;
import com.gome.pop.dto.es.EsOrder;
import com.gome.pop.dto.order.CartGoods;
import com.gome.pop.dto.order.Consignee;
import com.gome.pop.dto.order.Order;
import com.gome.pop.esearch_for_order.ESearchOperational;
import com.gome.pop.interfaze.order.service.CartGoodsService;
import com.gome.pop.remoteservice.order.esearch.ESearchOperationalImpl;
import com.gome.pop.remoteservice.order.esearch.ElasticSearchClientConfig;
import com.gome.pop.remoteservice.order.esearch.OrderBeanCovert;
import com.gome.pop.remoteservice.order.esearch.ScanUtil;
import com.gome.pop.remoteservice.order.esearch.canal.kafka.Producer;
import com.gome.pop.remoteservice.order.esearch.canal.monitor.Data4Monitor;
import com.google.protobuf.InvalidProtocolBufferException;

public class CanalListenner {

	protected  final static Logger logger=Logger.getLogger("scanSuccess");  
	
	private static List<String> list=OrderBeanCovert.es_order_property_list;
	
	private static ESearchOperational esSearchOperationalService;
	
	private static CartGoodsService cartGoodsService;
	
	private static OrderDao orderDao;
	
	public static OrderDao getOrderDao() {
		return orderDao;
	}
	
	public static void setOrderDao(OrderDao orderDao) {
		CanalListenner.orderDao = orderDao;
	}

	// 查询es和转换对象依赖的2个
	public ESearchOperational getEsSearchOperationalService() {
		return esSearchOperationalService;
	}

	public void setEsSearchOperationalService(
			ESearchOperational esSearchOperationalService) {
		CanalListenner.esSearchOperationalService = esSearchOperationalService;
	}

	public static CartGoodsService getCartGoodsService() {
		return cartGoodsService;
	}

	public static void setCartGoodsService(CartGoodsService cartGoodsService) {
		CanalListenner.cartGoodsService = cartGoodsService;
	}
	
	public static void synOrderStates(List<Entry> list){
		logger.error("开始解析事务中的所有表到oracle库====");
		Data4Monitor.modifyOracle(list);
		logger.error("完成解析事务中的所有表到oracle库====");
	}
	
	//监控tbl_tracking_no表变化,不管是新增还是修改都是修改es对象
	public static void onTrackingChange(String subOrderId,String trackingNo){
		 logger.error("新增或者修改tbl_tbl_tracking_no开始,通过subOrderId,trackingNo="+subOrderId+","+trackingNo);
		 EsOrder es_order=new EsOrder();
		 es_order.setSUB_ORDER_ID(subOrderId);
		 es_order.setTRACKING_NO(trackingNo);
		 esSearchOperationalService.updateEsearchByCondition(es_order);
		 logger.error("新增或者修改tbl_tbl_tracking_no结束");
	}
	
	
	public static void onInsert(List<Column> columns) {
		 logger.error("新增tbl_sub_order开始 ");
		 EsOrder es_order=readColumns(columns);
		 //找到新建订单关联的商品
		 Order pop_order =new Order();
		 pop_order.setSubOrderId(Long.valueOf(es_order.getSUB_ORDER_ID()));
		 List<Order> orders = new ArrayList<Order>();
		 orders.add(pop_order);
		 List<CartGoods> cart_goods_List=cartGoodsService.queryCartGoodsGroup(orders);
		 //构建es订单对象关联的商品sku和商品名称,从carts_goods表查询
		 List<String> sku=new ArrayList<String>();
		 List<String> goods_name=new ArrayList<String>();
		 for(CartGoods cartGood: cart_goods_List){
			 sku.add(cartGood.getSku().toString());
			 goods_name.add(cartGood.getGoodsName());
		 }
		 //创建es订单对象关联的收货人信息,从tbl_order_consignee表里查询
		 List<Consignee> consigneeList=  orderDao.queryOrderConsignee(pop_order);
		 if(consigneeList!=null && consigneeList.size()>0){
				es_order.setCONSIGNEE(consigneeList.get(0).getConsignee());
				es_order.setZIP_CODE(consigneeList.get(0).getZipCode());
				es_order.setMOBILE(consigneeList.get(0).getMobile());
				es_order.setPHONE(consigneeList.get(0).getPhone());
				es_order.setPROVINCE(consigneeList.get(0).getProvince());
				es_order.setCITY(consigneeList.get(0).getCity());
				es_order.setDISTRICT(consigneeList.get(0).getDistrict());
				es_order.setSTATE_NAME(consigneeList.get(0).getState_name());
				es_order.setADDRESS(consigneeList.get(0).getAddress());
				es_order.setEMAIL(consigneeList.get(0).getEmail());
				es_order.setDELIVERY_DATE(consigneeList.get(0).getDeliveryDate());
		 }
		boolean bool= esSearchOperationalService.insertEsearch(es_order,sku,goods_name);
		logger.error("新增tbl_sub_order结束:"+es_order.getSUB_ORDER_ID()+sku+goods_name+":"+bool);
	}
	
	public static void onUpdate(List<Column> columns,String subOrderId) {
		logger.error("修改tbl_sub_order开始,通过subOrderId="+subOrderId);
		EsOrder es_order=readColumns(columns);
		es_order.setSUB_ORDER_ID(subOrderId);
		boolean bool=esSearchOperationalService.updateEsearchByCondition(es_order);
		logger.error("修改tbl_sub_order结束,通过subOrderId="+es_order.getSUB_ORDER_ID()+":"+bool);
		
	}
	
	public static void onDelete(String subOrderId) {
		logger.error("删除tbl_sub_order开始,通过subOrderId="+subOrderId);
		boolean bool=esSearchOperationalService.deleteEsOrder(subOrderId);
		logger.error("删除tbl_sub_order结束,通过subOrderId="+subOrderId+":"+bool);
	}
	
	
	private static EsOrder readColumns(List<Column> columns) {
		EsOrder es_order=new EsOrder();
		try {
			for (Column column : columns) {
				//if (column.getUpdated()) {
					if(list.contains(column.getName().toUpperCase())&&!column.getValue().equals("")){
						PropertyDescriptor pd2 = new PropertyDescriptor(column.getName().toUpperCase(),es_order.getClass());  
						Method setter = pd2.getWriteMethod();  
						setter.invoke(es_order,column.getValue());
						logger.error(column.getName() + " : " + column.getValue()+ "    update=" + column.getUpdated());
					}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error(ScanUtil.getExceptionStack(e));
			e.printStackTrace();
		}
		return es_order;
	}


}
