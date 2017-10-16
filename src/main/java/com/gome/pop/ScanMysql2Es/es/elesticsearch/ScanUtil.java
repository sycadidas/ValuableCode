package com.gome.pop.ScanMysql2Es.es.elesticsearch;

public class ScanUtil {
	
	
	public static String getExceptionStack(Exception e) {
		StackTraceElement[] stackTraceElements = e.getStackTrace();
		String result = e.toString() + "\n";
		for (int index = stackTraceElements.length - 1; index >= 0; --index) {
			result += "at [" + stackTraceElements[index].getClassName() + ",";
			result += stackTraceElements[index].getFileName() + ",";
			result += stackTraceElements[index].getMethodName() + ",";
			result += stackTraceElements[index].getLineNumber() + "]\n";
		}
		return result;
	}
	
	
	/*
	 * ����ת��
	 */
	public static String formatTime(long ms) {
		int ss = 1000;
		int mi = ss * 60;
		int hh = mi * 60;
		int dd = hh * 24;

		long day = ms / dd;
		long hour = (ms - day * dd) / hh;
		long minute = (ms - day * dd - hour * hh) / mi;
		long second = (ms - day * dd - hour * hh - minute * mi) / ss;
		long milliSecond = ms - day * dd - hour * hh - minute * mi - second
				* ss;

		String strDay = day < 10 ? "0" + day : "" + day; // ��
		String strHour = hour < 10 ? "0" + hour : "" + hour;// Сʱ
		String strMinute = minute < 10 ? "0" + minute : "" + minute;// ����
		String strSecond = second < 10 ? "0" + second : "" + second;// ��
		String strMilliSecond = milliSecond < 10 ? "0" + milliSecond : ""
				+ milliSecond;// ����
		strMilliSecond = milliSecond < 100 ? "0" + strMilliSecond : ""
				+ strMilliSecond;

		return strMinute + " ���� " + strSecond + " ��";
	}
}
