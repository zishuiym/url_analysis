package com.datayes.telecom.eastmoney;

import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;

import org.htmlparser.Node;
import org.htmlparser.NodeFilter;
import org.htmlparser.Parser;
import org.htmlparser.filters.NodeClassFilter;
import org.htmlparser.filters.OrFilter;
import org.htmlparser.tags.LinkTag;
import org.htmlparser.util.NodeList;
import org.htmlparser.util.ParserException;

import com.datayes.tools.FileTool;

public class GenEastmoneyStockList {

	public static Hashtable<String, String> genEastMoneyStockList(String rootUrl) {
		Hashtable<String, String> codeToUrlMap = new Hashtable<String, String>();

		return codeToUrlMap;
	}

	public static Set<String> extractLinks(String url) {
		Set<String> links = new HashSet<String>();
		try {
			Parser parser = new Parser(url);
			parser.setEncoding("UTF-8");
			// parser.setEncoding("utf-8");
			// 过滤 <frame >标签的 filter，用来提取 frame 标签里的 src 属性所表示的链接
			NodeFilter frameFilter = new NodeFilter() {
				public boolean accept(Node node) {
					if (node.getText().startsWith("frame src=")) {
						return true;
					} else {
						return false;
					}
				}
			};
			// OrFilter 来设置过滤 <a> 标签，和 <frame> 标签
			OrFilter linkFilter = new OrFilter(new NodeClassFilter(
					LinkTag.class), frameFilter);
			// 得到所有经过过滤的标签
			NodeList list = parser.extractAllNodesThatMatch(linkFilter);
			for (int i = 0; i < list.size(); i++) {
				Node tag = list.elementAt(i);
				if (tag instanceof LinkTag)// <a> 标签
				{
					LinkTag link = (LinkTag) tag;
					String linkUrl = link.getLink();// url
					// if (filter.accept(linkUrl))
					links.add(linkUrl);
				} else// <frame> 标签
				{
					// 提取 frame 里 src 属性的链接如 <frame src="test.html"/>
					String frame = tag.getText();
					int start = frame.indexOf("src=");
					frame = frame.substring(start);
					int end = frame.indexOf(" ");
					if (end == -1)
						end = frame.indexOf(">");
					String frameUrl = frame.substring(5, end - 1);
					// if (filter.accept(frameUrl))
					links.add(frameUrl);
				}
			}
		} catch (ParserException e) {
			e.printStackTrace();
		}
		return links;
	}

	public static void main(String args[]) {
		String rootUrl = "http://quote.eastmoney.com/stock_list.html";
		Hashtable<String, String> codeToUrlMap = genEastMoneyStockList(rootUrl);
		Set<String> set = extractLinks(rootUrl);
		for (String url : set) {
			if ("http://quote.eastmoney.com/sz300412.html".length() == url.length())
			{
				FileTool.writeLine("lib/eastmoney/urls.txt", url);
				System.out.println(url);
			}
		}
	}
}
