package com.datayes.tools;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class FileTool {

	public static FileOutputStream fos;
	public static File f;
	
	public static void writeLine(String filePath, String txt)
	{
		f = new File(filePath);
		try {
			fos = new FileOutputStream(f,true);
			fos.write((txt+"\n").getBytes());
			fos.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void writeLineWithoutAppend(String filePath, String txt)
	{
		f = new File(filePath);
		try {
			fos = new FileOutputStream(f,false);
			fos.write((txt+"\n").getBytes());
			fos.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
