package com.gome.pop.ScanMysql2Es.es.elesticsearch;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;

import com.alibaba.fastjson.JSON;
import com.founder.bbc.diamond.DiamondOP;
import com.gome.pop.dto.es.EsOrder;
import com.gome.pop.dto.order.Order;
import com.gome.pop.esearch_for_order.ESearchOperational;

/**
 *
 * @ClassName: ESearchOperationalImpl
 * @Description:实现操作ES
 * @author sunyanchen
 * @date 2017年3月14日 上午10:59:01
 */
public class ESearchOperationalImpl implements ESearchOperational {

	private String index_Suffix = DiamondOP.getProperty("POP_ORDER", "ORDER_ES_CONF", "es_order_index", "");

	private String type_Suffix = DiamondOP.getProperty("POP_ORDER", "ORDER_ES_CONF", "es_order_type", "");

	private static Logger log = Logger.getLogger(ESearchOperationalImpl.class);

	private ElasticSearchClientConfig clientConfig;


	public ElasticSearchClientConfig getClientConfig() {
		return clientConfig;
	}

	public void setClientConfig(ElasticSearchClientConfig clientConfig) {
		this.clientConfig = clientConfig;
	}

	/**
	 * 插入操作 传入的Order对象要与ES文档Field对应
	 */
	@Override
	public Boolean insertEsearch(EsOrder order) {
		boolean insert_flag = false;
		try {
			XContentBuilder builder=getBuilder(order);
			builder.endObject();
			IndexResponse indexResponse = clientConfig.client.prepareIndex()
					.setIndex(index_Suffix).setType(type_Suffix)
					.setId(order.getSUB_ORDER_ID()) // 如果没有设置id，则ES会自动生成一个id
													// 这里是配送单号
					.setSource(builder.string()).get();
			insert_flag = indexResponse.isCreated();
		} catch (Throwable e) {
			log.error("插入记录报错");
			e.printStackTrace();
		}
		return insert_flag;
	}

	public Boolean insertEsearch(EsOrder order, List<String> skuList,
			List<String> goodsNameList) {
		boolean insert_flag = false;
		try {
			XContentBuilder builder=getBuilder(order);
			builder.field("SKU", skuList);
			builder.field("GOODS_NAME", goodsNameList);
			builder.endObject();


			IndexResponse indexResponse = clientConfig.client.prepareIndex()
					.setIndex(index_Suffix).setType(type_Suffix)
					.setId(order.getSUB_ORDER_ID()) // 如果没有设置id，则ES会自动生成一个id
													// 这里是配送单号
					.setSource(builder.string()).get();
			insert_flag = indexResponse.isCreated();
		} catch (Throwable e) {
			log.error("插入记录报错");
			e.printStackTrace();
		}

		return insert_flag;
	}

	private static XContentBuilder getBuilder(EsOrder order) throws Exception{
		XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
		Map<String, Object> order_map = transBean2Map(order);
		Set<String> order_keys = order_map.keySet();
		for (String k : order_keys) {
			builder.field(k, (String) order_map.get(k));
		}
		return builder;
	}

	/**
	 * 查询操作
	 */
	@Override
	public List<EsOrder> queryEsearchByCondition(EsOrder order, int startIndex,
			int pageSize) {
		List<EsOrder> list = null;

		SearchRequestBuilder searchRequest = clientConfig.client
				.prepareSearch(index_Suffix).setTypes(type_Suffix)
				.setSearchType(SearchType.QUERY_THEN_FETCH).setFrom(startIndex)
				.setSize(pageSize)// 分页
				.addSort("ORDER_DATE", SortOrder.DESC);// 排序

		try {
			// 创建关键字Queries
			QueryBuilder nameQueryBuilder = (QueryBuilder) produceSource(order,
					"Q");

			// 创建Response
			SearchResponse searchResponse = searchRequest.setQuery(
					nameQueryBuilder).get();

			SearchHits hits = searchResponse.getHits();
			list = new ArrayList<EsOrder>();

			if (hits != null && hits.getTotalHits() > 0) {
				for (SearchHit searchHit : hits) {
					list.add(JSON.parseObject(searchHit.getSourceAsString(),
							EsOrder.class));
				}
			}

		} catch (Exception e) {

		}
		return list;
	}

	/**
	 * 更新操作
	 */
	@Override
	public Boolean updateEsearchByCondition(EsOrder order) {
		boolean update_flag = true;

		XContentBuilder source;
		try {
			source = (XContentBuilder) produceSource(order, "U");
			clientConfig.client
					.prepareUpdate(index_Suffix, type_Suffix,
							order.getSUB_ORDER_ID()).setDoc(source).get();
		} catch (Exception e) {
			log.error("更新失败");
			update_flag = false;
			e.printStackTrace();
		}
		return update_flag;
	}

	/**
	 * 统计订单数量
	 */
	public Integer countOrder(EsOrder order) {
		SearchRequestBuilder searchRequest = clientConfig.client.prepareSearch(
				index_Suffix).setTypes(type_Suffix);
		// 创建关键字Queries
		QueryBuilder nameQueryBuilder = (QueryBuilder) produceSource(order, "Q");

		// 创建Response
		SearchResponse searchResponse = searchRequest
				.setQuery(nameQueryBuilder).get();

		return (int) searchResponse.getHits().getTotalHits();
	}

	/**
	 * 删除一条记录
	 */
	public boolean deleteEsOrder(String sub_order_id) {
		DeleteResponse response = clientConfig.client
				.prepareDelete(index_Suffix, type_Suffix, sub_order_id)
				.execute().actionGet();
		return response.isFound();
	}

	/**
	 * 
	 * @Title: produceSource
	 * @Description:通过入参对象得到更新source
	 * @param order
	 * @param @return 设定文件
	 * @return XContentBuilder 返回类型
	 * @throws
	 */
	private Object produceSource(EsOrder order, String sign) {
		XContentBuilder builder = null;// 更新条件
		BoolQueryBuilder nameQueryBuilder = null;// 查询条件
		try {
			nameQueryBuilder = QueryBuilders.boolQuery();
			builder = XContentFactory.jsonBuilder().startObject();

			// 时间段查询
			rangeQuery(order, nameQueryBuilder, "ORDER_DATE");
			rangeQuery(order, nameQueryBuilder, "EX_TIME");
			rangeQuery(order, nameQueryBuilder, "DLRT_TIME");

			Map<String, Object> order_map = transBean2Map(order);
			for (String k : order_map.keySet()) {
				if ("ORDER_DATE_START".equals(k) || "EX_TIME_START".equals(k)
						|| "DLRT_TIME_START".equals(k)
						|| "ORDER_DATE_END".equals(k)
						|| "EX_TIME_END".equals(k) || "DLRT_TIME_END".equals(k)) {
					// 查询已处理，更新条件不包含这几个字段 不处理
				} else {
					if ("ORDER_STATE".equals(k)) {

						// String status = order_map.get(k).toString();
						String status = ((order_map.get(k) != null) ? order_map
								.get(k).toString() : null);

						if (status != null && !status.equals("1")&& !status.equals("3")
								&& !status.equals("4") && !status.equals("7")) {
							builder.field(k, status);
							nameQueryBuilder.must(QueryBuilders.termQuery(k,
									status));
						} else if (status == null) {
							// 不做任何操作
						} else {
							switch (status) {
							
							case "1":
								break;
							case "3":
								nameQueryBuilder.must(QueryBuilders
										.boolQuery()
										.should(QueryBuilders.termQuery(
												"ORDER_STATE", "PP"))
										.should(QueryBuilders.termQuery(
												"ORDER_STATE", "PR")));
								break;
							case "4":
								nameQueryBuilder.must(QueryBuilders
										.boolQuery()
										.should(QueryBuilders.termQuery(
												"ORDER_STATE", "CWS"))
										.should(QueryBuilders.termQuery(
												"ORDER_STATE", "CWC")));
								break;
							case "7":
								nameQueryBuilder.must(QueryBuilders
										.boolQuery()
										.should(QueryBuilders.termQuery(
												"ORDER_STATE", "CL"))
										.should(QueryBuilders.termQuery(
												"ORDER_STATE", "RV"))
										.should(QueryBuilders.termQuery(
												"ORDER_STATE", "RT"))
										.should(QueryBuilders.termQuery(
												"ORDER_STATE", "DFC"))
										.should(QueryBuilders.termQuery(
												"ORDER_STATE", "RCC")));
								break;
							}
						}
					} else {
						if (order_map.get(k) != null) {
							builder.field(k, (String) order_map.get(k));
							nameQueryBuilder.must(QueryBuilders.termQuery(k,
									(String) order_map.get(k)));
						}
					}
				}
			}

			builder.endObject();
		} catch (IOException e) {
			e.printStackTrace();
		}
		if (sign.equals("U")) {
			return builder;
		} else {
			return nameQueryBuilder;
		}
	}

	// 范围查询
	private void rangeQuery(EsOrder order, BoolQueryBuilder nameQueryBuilder,
			String dateType) {
		if ("ORDER_DATE".equals(dateType)) {
			// 下单时间动态拼接范围查询参数
			String ods = order.getORDER_DATE_START();
			String ode = order.getORDER_DATE_END();
			RangeQueryBuilder rqb_ordr = QueryBuilders.rangeQuery(dateType);
			if (ods != null && ode != null) {
				rqb_ordr.gte(ods).lte(ode);
				nameQueryBuilder.filter(rqb_ordr);
			} else if (ods != null && ode == null) {
				rqb_ordr.gte(ods);
				nameQueryBuilder.filter(rqb_ordr);
			} else if (ods == null && ode != null) {
				rqb_ordr.lte(ode);
				nameQueryBuilder.filter(rqb_ordr);
			} else if (ods == null && ode == null) {
			}
		} else if ("EX_TIME".equals(dateType)) {
			String ets = order.getEX_TIME_START();
			String ete = order.getEX_TIME_END();
			RangeQueryBuilder rqb_ex = QueryBuilders.rangeQuery(dateType);
			if (ets != null && ete != null) {
				rqb_ex.gte(ets).lte(ete);
				nameQueryBuilder.filter(rqb_ex);
			} else if (ets != null && ete == null) {
				rqb_ex.gte(ets);
				nameQueryBuilder.filter(rqb_ex);
			} else if (ets == null && ete != null) {
				rqb_ex.lte(ete);
				nameQueryBuilder.filter(rqb_ex);
			} else if (ets == null && ete == null) {
			}
		} else if ("DLRT_TIME".equals(dateType)) {
			String dts = order.getDLRT_TIME_START();
			String dte = order.getDLRT_TIME_END();
			RangeQueryBuilder rqb_dl = QueryBuilders.rangeQuery(dateType);
			if (dts != null && dte != null) {
				rqb_dl.gte(dts).lte(dte);
				nameQueryBuilder.filter(rqb_dl);
			} else if (dts != null && dte == null) {
				rqb_dl.gte(dts);
				nameQueryBuilder.filter(rqb_dl);
			} else if (dts == null && dte != null) {
				rqb_dl.lte(dte);
				nameQueryBuilder.filter(rqb_dl);
			} else if (dts == null && dte == null) {
			}
		}
	}

	/**
	 * 
	 * @Title: transBean2Map
	 * @Description: 利用Introspector和PropertyDescriptor 将Bean --> Map
	 * @param @param obj
	 * @return Map<String,Object> 返回类型
	 * @author sunyanchen
	 */
	public static Map<String, Object> transBean2Map(Object obj) {
		if (obj == null) {
			return null;
		}
		Map<String, Object> map = new HashMap<String, Object>();
		try {
			BeanInfo beanInfo = Introspector.getBeanInfo(obj.getClass());
			PropertyDescriptor[] propertyDescriptors = beanInfo
					.getPropertyDescriptors();
			for (PropertyDescriptor property : propertyDescriptors) {
				String key = property.getName();
				// 过滤class属性
				if (!key.equals("class")) {
					// 得到property对应的getter方法
					Method getter = property.getReadMethod();
					Object value = getter.invoke(obj);
					map.put(key, value);
				}
			}
		} catch (Exception e) {
			log.error("转换错误");
		}
		return map;

	}

}
