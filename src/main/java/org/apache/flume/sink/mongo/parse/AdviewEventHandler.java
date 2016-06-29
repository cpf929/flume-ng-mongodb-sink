package org.apache.flume.sink.mongo.parse;

import com.alibaba.fastjson.JSONObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;

/**
 * 处理adview的日志
 * 
 * @author chenpf
 *
 */
public class AdviewEventHandler implements EventHandler {

	public DBObject buildDbObject(JSONObject jsonObject) {

		BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();

		return builder.get();
	}

	
	public boolean check(JSONObject jsonObject) {
		// TODO Auto-generated method stub
		return false;
	}

}
