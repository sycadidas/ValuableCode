package com.gome.pop.ScanMysql2Es.es.elesticsearch;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.commons.beanutils.ConvertUtils;
import org.apache.log4j.Logger;


import com.gome.pop.dto.es.EsOrder;
import com.gome.pop.dto.order.Order;
import com.gome.pop.dto.order.OrderStatusConverter;

public class OrderBeanCovert {
	
	private static Logger log = Logger.getLogger(OrderBeanCovert.class);
	
	//goodsNo没用,查询的时候用的是sku,需要存储把订单关联的商品的id存储到sku中,商品名称存储到goods_name.查询完订单库,有一段代码专门把订单关联的商品又查询了一次.
	//CONSIGNEE对应了order的consigneeMan,查询用到了.(展示的时候是否显示不知道?????)
	
	//如果觉得放这里不好看,随意挪到任意地方去，但是要求保证array中的item是有序的
	public  static  List<String> es_order_property_list = new ArrayList<String>(Arrays.asList(
			"ORDER_GOODS_TYPE", "DLRT_TIME", "SALE_TAX", "VAT_TAX", "COMPREHENSIVE_TAX", "ORDER_NO", "SUB_ORDER_ID","ORDER_ORIGIN","SUB_ORDER_TYPE","TOTAL_PRICE",
			"ORDER_DATE", "ORDER_STATE", "ORDER_STATE_TIME", "REMARK", "OPINION_DESC", "PAY_MODEL", "FREIGHT","PAY_SATE","OWN_SHOP","GOME_TRACKING",
			"SHOP_NO", "INVOICE_TYPE", "PART_DISCOUNT_PRICE", "COUPON_VALUE", "SHOP_NAME", "CUSTMER_TAX_NUM", "GOME_FREIGHT","IS_INVOICED","HEAD_TYPE","WAREHOUSE_NAME",
			"WAREHOUSE_ID", "RECEIVE_TIME", "TWO_DELIVERY_TIME", "TWO_DELIVERY_DESC", "TWO_DELIVERY_FLAG", "RECEIVABLE_AMOUNT", "ABFLAG","PRE_PAYMENT","EX_TIME","BLACKLIST_ID",
			"BLACKLIST_TYPE", "STATUS_REASON_DESC", "REASON", "NO_EX_REASON", "EXPECT_EX_DATE", "COD_TERM", "SHOP_FLAG","TOTAL_POSTAL_TAX","ID_NAME","ID_NUM",
			"TAKED_EXPRESS_TIME", "SIGNED_TIME", "APPRAISE_FLAG", "REMARK_LEVEL", "OPERATOR_DESC", "PAYMENT_DISCOUNT", "ZIP_CODE","CONSIGNEE","MOBILE","PHONE",
			"PROVINCE", "CITY", "DISTRICT",  "STATE_NAME","ADDRESS","EMAIL","DELIVERY_DATE", 
			"GOODS_NAME", "SKU",//这两个字段存储的时候使用array,匹配页面上的查询商品名称和商品sku的输入框
			"TRACKING_NO",
			"ORDER_DATE_START","ORDER_DATE_END","EX_TIME_START","EX_TIME_END","DLRT_TIME_START", "DLRT_TIME_END"));//这几个字段是为了匹配页面上的时间范围查询

	public   static List<String> order_property_list = new ArrayList<String>(Arrays.asList(
			"orderGoodsType", "dlrtTime", "saleTax", "vatTax", "comprehensiveTax", "orderNo", "subOrderId","orderOrigin","subOrderType","totalPrice",
			"orderDate", "orderState", "orderStateTime", "remark", "opinionDesc", "payModel", "freight","paySate","ownShop","gomeTracking",
			"shopNo", "invoiceType", "partDiscountPrice", "couponValue", "shopName", "custmerTaxNum", "gome_freight","isInvoiced","headType","warehouse",
			"warehouseId", "receiveTime", "two_delivery_time", "two_delivery_desc", "two_delivery_flag", "receivableAmount", "abFlag","prePayment","exTime","blackListId",
			"blackListType", "statusReasonDesc", "reason", "noExReason", "expectExDate", "codTerm", "shopFlag","total_postal_tax","idName","idNum",
			"takedExpressDate", "signedDate", "appraise_flag", "remarkLevel", "operator_desc", "paymentDiscount", "zipCode","consigneeMan","mobile","phone",
			"province", "city", "district", "state_name","address","email","deliveryDate", 
			"goodsName", "sku",
			"trackingNo",
			"orderDateStart", "orderDateEnd","orderStorageDateStart","orderStorageDateEnd","dlrtTimeStart", "dlrtTimeEnd"));
	
	//另外一种初始化的方法
//	final List<String> order = new ArrayList<String>() {
//		{
//			add("saleTax");
//	        add("vatTax");
//			
//		}
//	};
	
	public static Object covertBean(Object sourceObject,String type) {
		
		Object source;
		Object target=new Object();
		List source_list=new ArrayList(80);
		List target_list=new ArrayList(80);
		
		//查询的时候,从order转换到esorder
		if(type.equals("query")){
			source=sourceObject;
			
			source_list=order_property_list;
			
			source_list.set(15, "payModel2");
			
			target=new EsOrder();
			target_list=es_order_property_list;
			
		}
		//返回的时候,从esorder转换到order
		else{
			source=sourceObject;
			source_list=es_order_property_list;
			target=new Order();
			
			//为了满足页面上的monitorTag监控预警
			long now=new Date().getTime();
			
			((Order)target).setSysdatePR(new Timestamp(now-1000*60*30));//
			((Order)target).setSysdatePP(new Timestamp(now-1000*60*60*24));
			((Order)target).setSysdateEX(new Timestamp(now-1000*60*60*24*7));
			((Order)target).setSysdateCWS(new Timestamp(now-1000*60*60*24));
			((Order)target).setSysdateRV(new Timestamp(now-1000*60*60*24));
			
			target_list=order_property_list;
			
			
		}
		
		try {
			PropertyDescriptor[] propertyDescriptors = Introspector.getBeanInfo(source.getClass()).getPropertyDescriptors();
			for (PropertyDescriptor property : propertyDescriptors) {
				String key = property.getName();
				// 过滤class属性
				if (!key.equals("class")) {
					//循环时,如果发现属性需要做转换
					if(source_list.contains(key)){
						//得到属性在array中的位置
						int index=source_list.indexOf(key);
						// 得到property对应的getter方法,反射获取值
						Method getter = property.getReadMethod();
						Object value = getter.invoke(source);
						if(value!=null&&!value.equals("")){
							//反射放到target对象去
							PropertyDescriptor pd2 = new PropertyDescriptor(target_list.get(index).toString(),target.getClass());  
							//System.out.println(key+":type="+property.getPropertyType().toString()+"::::"+value+"-------------->"+target_list.get(index)+":type="+	pd2.getPropertyType().toString());
							Method setter = pd2.getWriteMethod();  
							//注意转换的时候的field的类型
							value=ConvertUtils.convert(String.valueOf(value), pd2.getPropertyType());
							setter.invoke(target, value);
						}
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			log.error(ScanUtil.getExceptionStack(e));
		}
		return target;
	}
}
