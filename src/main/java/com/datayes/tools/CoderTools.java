package com.datayes.tools;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

public class CoderTools {

	public static void main(String args[])
	{
		String encodedKeyword;
		try {
			encodedKeyword = URLEncoder.encode("一带一路","UTF-8");
			System.out.println(encodedKeyword);
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
