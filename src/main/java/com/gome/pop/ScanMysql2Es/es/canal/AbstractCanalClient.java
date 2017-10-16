package com.gome.pop.ScanMysql2Es.es.canal;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.SystemUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.slf4j.MDC;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.alibaba.otter.canal.protocol.Message;

public class AbstractCanalClient {
	
	protected  final static Logger logger=Logger.getLogger("scanSuccess");  
	
	protected static final String SEP = SystemUtils.LINE_SEPARATOR;
	protected static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
	protected volatile boolean running = false;
	protected Thread.UncaughtExceptionHandler handler = new Thread.UncaughtExceptionHandler() {
		public void uncaughtException(Thread t, Throwable e) {
			logger.error("parse events has an error", e);
		}
	};
	protected Thread thread = null;
	protected CanalConnector connector;
	protected String destination;

	public AbstractCanalClient() {

	}

	public AbstractCanalClient(String destination) {
		this(destination, null);
	}

	public AbstractCanalClient(String destination, CanalConnector connector) {
		this.destination = destination;
		this.connector = connector;
	}

	protected void start() {
		Assert.notNull(connector, "connector is null");
		thread = new Thread(new Runnable() {
			public void run() {
				process();
			}
		});

		thread.setUncaughtExceptionHandler(handler);
		thread.start();
		running = true;
	}

	protected void stop() {
		if (!running) {
			return;
		}
		running = false;
		if (thread != null) {
			try {
				thread.join();
			} catch (InterruptedException e) {
				logger.error("CANAL-CLIENT THREAD FAULT:"+ExceptionUtils.getFullStackTrace(e));
			}
		}
		MDC.remove("destination");
	}

	protected void process() {
		int batchSize = 5 * 1024;
		while (running) {
			try {
				MDC.put("destination", destination);
				connector.connect();
				
				connector.subscribe("");
				while (running) {
					Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据
					long batchId = message.getId();
					int size = message.getEntries().size();
					if (batchId == -1 || size == 0) {
//						try {
//							Thread.sleep(1000);
//						} catch (InterruptedException e) {
//						}
					} else {
						logger.error("==================解析"+batchId+"开始=====================");
						printSummary(message, batchId, size);//记录日志
						printEntry(message.getEntries());//根据binlog事件 同步至ES
//						CanalListenner.synOrderStates(message.getEntries());//提供数据给监控---暂时先不提供 避免上线初期有问题 影响监控
						logger.error("==================解析"+batchId+"结束=====================");
					}
					connector.ack(batchId); // 提交确认
					if(batchId!=-1)
						logger.error(batchId);
					// connector.rollback(batchId); // 处理失败, 回滚数据
				}
			} catch (Exception e) {
				e.printStackTrace();
				logger.error("process error!", e);
			} finally {
				connector.disconnect();
				MDC.remove("destination");
			}
		}
	}

	private static void printEntry(List<Entry> entrys) {
		logger.error("开始解析事务中的所有表到es====");
		for (Entry entry : entrys) {
			
			if (entry.getEntryType() == EntryType.ROWDATA) {

				RowChange rowChage = null;
				try {
					rowChage = RowChange.parseFrom(entry.getStoreValue());
				} catch (Exception e) {
					logger.error("ERROR ## parser of eromanga-event has an error , data:"+ entry.toString(), e);
				}

				EventType eventType = rowChage.getEventType();
				
				String table_name=entry.getHeader().getTableName();
				
				if(eventType!=EventType.QUERY){
					logger.error("开始----现在解析的表名叫:"+String.valueOf(table_name)+";当前表操作的类型是:"+eventType);
					// 跟着tbl_sub_order和tbl_return_order
					if ("tbl_sub_order".equals(table_name)) {
						for (RowData rowData : rowChage.getRowDatasList()) {
							if (eventType == EventType.DELETE) {
								CanalListenner.onDelete(rowData.getBeforeColumns(0).getValue());
							} else if (eventType == EventType.INSERT) {
								CanalListenner.onInsert(rowData.getAfterColumnsList());
							} else if (eventType == EventType.UPDATE) {
								CanalListenner.onUpdate(rowData.getAfterColumnsList(), rowData.getAfterColumns(0).getValue());
							}
						}
					}
					else if("tbl_tracking_no".equals(table_name)){
						for (RowData rowData : rowChage.getRowDatasList()) {
							if (eventType == EventType.INSERT||eventType == EventType.UPDATE) {
							CanalListenner.onTrackingChange(rowData.getAfterColumns(1).getValue(), rowData.getAfterColumns(5).getValue());
							}
						}
					}
					logger.error("结束----现在解析的表名叫:"+String.valueOf(table_name)+";当前表操作的类型是:"+eventType);
				}
				}
		}
		logger.error("完成解析事务中的所有表到es====");
	}

	private void printSummary(Message message, long batchId, int size) {
		String startPosition = null;
		String endPosition = null;
		if (!CollectionUtils.isEmpty(message.getEntries())) {
			startPosition = buildPositionForDump(message.getEntries().get(0));
			endPosition = buildPositionForDump(message.getEntries().get(
					message.getEntries().size() - 1));
		}
		SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
		logger.error("开始同步es:batchId:"+batchId+";size:"+size+";开始同步时间:"+
				format.format(new Date())+";开始位置:"+startPosition+";结束位置:"+endPosition );
	}

	protected String buildPositionForDump(Entry entry) {
		long time = entry.getHeader().getExecuteTime();
		Date date = new Date(time);
		SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
		return entry.getHeader().getLogfileName() + ":"
				+ entry.getHeader().getLogfileOffset() + ":"
				+ entry.getHeader().getExecuteTime() + "("
				+ format.format(date) + ")";
	}

	protected void printColumn(List<Column> columns) {
		for (Column column : columns) {
			StringBuilder builder = new StringBuilder();
			builder.append(column.getName() + " : " + column.getValue());
			builder.append("    type=" + column.getMysqlType());
			if (column.getUpdated()) {
				builder.append("    update=" + column.getUpdated());
			}
			builder.append(SEP);
			logger.info(builder.toString());
		}
	}

	public void setConnector(CanalConnector connector) {
		this.connector = connector;
	}
}
