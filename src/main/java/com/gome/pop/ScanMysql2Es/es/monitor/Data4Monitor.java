package com.gome.pop.ScanMysql2Es.es.monitor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.founder.bbc.diamond.DiamondOP;
import com.gome.pop.dto.es.EsOrder;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * 为了监控订单状态,把订单数据提供给监控
 * 
 * @author renjin
 *
 */
public class Data4Monitor {

	protected  final static Logger logger=Logger.getLogger("scanSuccess");  
	
	
	public static void modifyOracle(List<Entry> list) {
		
		DataBaseDao dao = new DataBaseDao();
		
		List<Connection> conList = new ArrayList<Connection>();
		//数据源连接
//		Connection con1 = dao.getConnection("jdbc:oracle:thin:@10.58.66.36:31521:bodb1","oms_uat","mKjHXvzTCmgWy1k");
//		Connection con2 = dao.getConnection("jdbc:oracle:thin:@10.58.66.37:31521:bodb2","oms_uat","mKjHXvzTCmgWy1k");
//		Connection con3 = dao.getConnection("jdbc:oracle:thin:@10.58.46.17:31521:repdb","oms_uat","mKjHXvzTCmgWy1k");
//		Connection con4 = dao.getConnection("jdbc:oracle:thin:@10.58.12.150:31521:slavedb","oms_uat","mKjHXvzTCmgWy1k");
		
		Connection con1 = dao.getConnection(DiamondOP.getProperty("POP_ORDER", "ORDER_ES_CONF", "orcl_url1", ""),
				                            DiamondOP.getProperty("POP_ORDER", "ORDER_ES_CONF", "orcl_user1", ""),
				                            DiamondOP.getProperty("POP_ORDER", "ORDER_ES_CONF", "orcl_pwd1", ""));
		
		Connection con2 = dao.getConnection(DiamondOP.getProperty("POP_ORDER", "ORDER_ES_CONF", "orcl_url2", ""),
											DiamondOP.getProperty("POP_ORDER", "ORDER_ES_CONF", "orcl_user2", ""),
											DiamondOP.getProperty("POP_ORDER", "ORDER_ES_CONF", "orcl_pwd2", ""));
		
		Connection con3 = dao.getConnection(DiamondOP.getProperty("POP_ORDER", "ORDER_ES_CONF", "orcl_url3", ""),
											DiamondOP.getProperty("POP_ORDER", "ORDER_ES_CONF", "orcl_user3", ""),
											DiamondOP.getProperty("POP_ORDER", "ORDER_ES_CONF", "orcl_pwd3", ""));
		
		Connection con4 = dao.getConnection(DiamondOP.getProperty("POP_ORDER", "ORDER_ES_CONF", "orcl_url4", ""),
											DiamondOP.getProperty("POP_ORDER", "ORDER_ES_CONF", "orcl_user4", ""),
											DiamondOP.getProperty("POP_ORDER", "ORDER_ES_CONF", "orcl_pwd4", ""));
		
		conList.add(con1);
		conList.add(con2);
		conList.add(con3);
		conList.add(con4);
		List<String> sqls=creatSQL(list);
		logger.error("要执行的sql是:"+sqls.toString());
		for (Connection connection : conList) {
			if(sqls.size()>0){
				Statement ps = null;
				try {
					connection.setAutoCommit(false);
					ps = connection.createStatement();
					for(String sql:sqls){
						ps.execute(sql);
					}
					connection.commit();
				} catch (SQLException e) {
					logger.error(e);
					// TODO Auto-generated catch block
					e.printStackTrace();
				} finally {
					if (connection != null)
						try {
							connection.close();
						} catch (SQLException e) {
							logger.error(e);
							e.printStackTrace();
						}
				}
			}
		}
		
	}

	private static List<String> creatSQL(List<Entry> list) {
		
		List<String> sqls=new ArrayList<String>();
	
		for (Entry entry : list) {

			String table_name = entry.getHeader().getTableName();
			EventType eventType = entry.getHeader().getEventType();

			RowChange rowChage = null;
			try {
				rowChage = RowChange.parseFrom(entry.getStoreValue());
			} catch (InvalidProtocolBufferException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			for (RowData rowData : rowChage.getRowDatasList()) {

				if ("tbl_sub_order".equals(table_name)) {
					if (eventType == EventType.DELETE) {
						String sql="DELETE from "+table_name+" where (SUB_ORDER_ID="+ rowData.getBeforeColumns(0).getValue()+") and (OWN_SHOP="+rowData.getBeforeColumns(5).getValue()+")"; 
						sqls.add(sql);
					} else if (eventType == EventType.INSERT) {
						StringBuilder str=new StringBuilder();
						str.append(" (SUB_ORDER_ID,ORDER_NO,ORDER_STATE,OWN_SHOP,SUB_ORDER_TYPE,ORDER_STATE_TIME,ORDER_DATE )  ");
						str.append(" values ");
						str.append(" ( ");
						str.append("'"+rowData.getAfterColumns(0).getValue()+"',");
						str.append("'"+rowData.getAfterColumns(1).getValue()+"',");
						str.append("'"+rowData.getAfterColumns(3).getValue()+"',");
						str.append("'"+rowData.getAfterColumns(5).getValue()+"',");
						str.append("'"+rowData.getAfterColumns(8).getValue()+"',");
						str.append("to_date('"+rowData.getAfterColumns(9).getValue()+"','yyyy-mm-dd hh24:mi:ss'),");
						str.append("to_date('"+rowData.getAfterColumns(10).getValue()+"','yyyy-mm-dd hh24:mi:ss') )");
						String sql="INSERT INTO "+table_name+ str.toString();
						sqls.add(sql);
					} else if (eventType == EventType.UPDATE) {
						StringBuilder str=new StringBuilder();
						Map<String, String> map=new HashMap<String, String>();
						if (rowData.getAfterColumns(1).getUpdated())
							map.put("ORDER_NO", rowData.getAfterColumns(1).getValue());
						if (rowData.getAfterColumns(3).getUpdated())
							map.put("ORDER_STATE", rowData.getAfterColumns(3).getValue());
						if (rowData.getAfterColumns(5).getUpdated())
							map.put("OWN_SHOP", rowData.getAfterColumns(5).getValue());
						if (rowData.getAfterColumns(8).getUpdated())
							map.put("SUB_ORDER_TYPE", rowData.getAfterColumns(8).getValue());
						if (rowData.getAfterColumns(9).getUpdated())
						    map.put("ORDER_STATE_TIME", rowData.getAfterColumns(9).getValue());
						if (rowData.getAfterColumns(10).getUpdated())
						    map.put("ORDER_DATE", rowData.getAfterColumns(10).getValue());
						
						String sp = "";
						for(Map.Entry<String, String> obj : map.entrySet())    
						{    
							str.append(sp);
							if(obj.getKey().equals("ORDER_STATE_TIME")||obj.getKey().equals("ORDER_DATE"))
								str.append(obj.getKey()+"=to_date('"+obj.getValue()+"','yyyy-mm-dd hh24:mi:ss')");
							else
								str.append(obj.getKey()+"='"+obj.getValue()+"'");
							sp = ",";
						} 
						
						if(null!=str.toString()&&!"".equals(str.toString())){
							String sql=" UPDATE "+table_name+" set "+str.toString()+" where (SUB_ORDER_ID="+ rowData.getBeforeColumns(0).getValue()+") and (OWN_SHOP="+rowData.getBeforeColumns(5).getValue()+")"; 
							sqls.add(sql);
						}
					}
				} else if ("tbl_return_order".equals(table_name)) {
					if (eventType == EventType.DELETE) {
						String sql="DELETE from "+table_name+" where ( ORDER_NO="+ rowData.getBeforeColumns(0).getValue() +")";
						sqls.add(sql);
					} else if (eventType == EventType.INSERT) {
						StringBuilder str=new StringBuilder();
						str.append(" ( ORDER_NO,ORDER_TYPE,STATUS,SUB_ORDER_ID,ORIGIN_ORDER,LAST_STATUS_TIME )  ");
						str.append(" values ");
						str.append(" ( ");
						str.append("'"+rowData.getAfterColumns(0).getValue()+"',");
						str.append("'"+rowData.getAfterColumns(1).getValue()+"',");
						str.append("'"+rowData.getAfterColumns(6).getValue()+"',");
						str.append("'"+rowData.getAfterColumns(7).getValue()+"',");
						str.append("'"+rowData.getAfterColumns(8).getValue()+"',");
						str.append("to_date('"+rowData.getAfterColumns(27).getValue()+"','yyyy-mm-dd hh24:mi:ss') ) ");
						String sql="INSERT INTO "+table_name+ str.toString();
						sqls.add(sql);
					} else if (eventType == EventType.UPDATE) {
						StringBuilder str=new StringBuilder();
						Map<String, String> map=new HashMap<String, String>();
						if (rowData.getAfterColumns(1).getUpdated())
							map.put("ORDER_TYPE", rowData.getAfterColumns(1).getValue());
						if (rowData.getAfterColumns(6).getUpdated())
							map.put("STATUS", rowData.getAfterColumns(6).getValue());
						if (rowData.getAfterColumns(7).getUpdated())
							map.put("STATUS", rowData.getAfterColumns(7).getValue());
						if (rowData.getAfterColumns(8).getUpdated())
							map.put("ORIGIN_ORDER", rowData.getAfterColumns(8).getValue());
						if (rowData.getAfterColumns(27).getUpdated())
							map.put("LAST_STATUS_TIME", rowData.getAfterColumns(27).getValue());
						
						String sp = "";
						for(Map.Entry<String, String> obj : map.entrySet())    
						{    
							str.append(sp);
							if(obj.getKey().equals("LAST_STATUS_TIME"))
								str.append(obj.getKey()+"=to_date('"+obj.getValue()+"','yyyy-mm-dd hh24:mi:ss')");
							else
								str.append(obj.getKey()+"='"+obj.getValue()+"'");
							sp = ",";
						} 
						
						if(null!=str.toString()&&!"".equals(str.toString())){
							String sql="UPDATE "+table_name+" set "+str.toString()+" where ( ORDER_NO="+ rowData.getBeforeColumns(0).getValue() +")"; 
							sqls.add(sql);
						}
					}
				}
			}
		}
		return sqls;
	}
}
