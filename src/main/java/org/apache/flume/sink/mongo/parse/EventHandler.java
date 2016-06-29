package org.apache.flume.sink.mongo.parse;

import com.alibaba.fastjson.JSONObject;
import com.mongodb.DBObject;

public interface EventHandler {

	/**
	 * 根据事件， 构建出写入mongodb的对象
	 * 
	 * @param jsonObject
	 * @return
	 */
	DBObject buildDbObject(JSONObject jsonObject);

	/**
	 * 校验数据的合法性
	 * 
	 * @param jsonObject
	 * @return 符合要求， true， 不符合， false
	 */
	boolean check(JSONObject jsonObject);
}
