package com.datayes.tools;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;

public class CoderTools {

	public static String decode(String orinStr, String coding) throws UnsupportedEncodingException
	{
		String resStr=orinStr.trim();
		if(resStr.contains("/"))
			resStr = resStr.split("/")[0];
		if(resStr.contains("?"))
			resStr = resStr.split("\\?")[0];
		resStr = URLDecoder.decode(resStr,coding);
		if(resStr.contains("/"))
			resStr = resStr.split("/")[0];
		if(resStr.contains("?"))
			resStr = resStr.split("\\?")[0];
		if(resStr.split("%").length>1)
		{
			resStr = URLDecoder.decode(resStr,coding);
		}
		resStr = resStr.trim();
		return resStr;
	}
	
	public static void main(String args[])
	{
		String encodedKeyword;
		try {
			String dd = decode("%25E4%25B8%2580%25E5%2586%259C?tracker_u=10403234939&adgroupKeywordID=30943184","UTF-8");
			System.out.println(dd);
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
