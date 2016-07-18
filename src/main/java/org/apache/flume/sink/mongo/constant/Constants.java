package org.apache.flume.sink.mongo.constant;

import java.util.ArrayList;
import java.util.List;

public class Constants {

//	public static final String set = "$set";
//	public static final String setOnInsert = "$setOnInsert";
//	public static final String addToSet = "$addToSet";
	
	
	public final static List<String> genderList = new ArrayList<String>();
	
	static{
		genderList.add("M");
		genderList.add("F");
	}
	
	
}
