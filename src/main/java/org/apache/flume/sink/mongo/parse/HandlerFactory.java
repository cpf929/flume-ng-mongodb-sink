package org.apache.flume.sink.mongo.parse;

import java.io.UnsupportedEncodingException;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Event;
import org.apache.flume.sink.mongo.constant.AdxType;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.mongodb.DBObject;

public class HandlerFactory {

	static EventHandler getHandler(String bsdAdxType) {

		EventHandler handler = null;

		if (AdxType.TANX.getKey().equalsIgnoreCase(bsdAdxType)) {
			handler = new TanxEventHandler();
		} else if (AdxType.ADVIEW.getKey().equalsIgnoreCase(bsdAdxType)) {
			handler = new AdviewEventHandler();
		}

		return handler;
	}

	public static DBObject build(Event event) throws UnsupportedEncodingException {

		String logJson = new String(event.getBody(), "UTF-8");

		JSONObject jsonObject = JSON.parseObject(logJson);

		String bsdAdxType = jsonObject.getString("bsdAdxType");

		if (StringUtils.isBlank(bsdAdxType)) {
			return null;
		}
		EventHandler handler = getHandler(bsdAdxType);

		return handler.buildDbObject(jsonObject);
	}

}
