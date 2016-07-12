package org.apache.flume.sink.mongo.parse;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.sink.mongo.MongoSink;
import org.apache.flume.sink.mongo.constant.FieldName;
import org.apache.flume.sink.mongo.constant.InfoType;

import com.alibaba.fastjson.JSONObject;
import com.mongodb.BasicDBObject;
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

		if (!check(jsonObject)) {
			return null;
		}

		BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();
		JSONObject device = jsonObject.getJSONObject(FieldName.Adview.device);

		String deviceId;
		if (StringUtils.isNotBlank(device.getString(FieldName.Adview.didsha1))) {
			deviceId = device.getString(FieldName.Adview.didsha1);
		} else {
			deviceId = device.getString(FieldName.Adview.dpidsha1);
		}

		// 新增， 不更新的
		BasicDBObject setObj = new BasicDBObject(FieldName.field_deviceIdSha1, deviceId);

		// brand
		if (StringUtils.isNotBlank(device.getString(FieldName.Adview.make))) {
			setObj.append(FieldName.field_brand, device.getString(FieldName.Adview.make));
		}
		// model
		if (StringUtils.isNotBlank(device.getString(FieldName.Adview.model))) {
			setObj.append(FieldName.field_model, device.getString(FieldName.Adview.model));
		}

		// os
		if (StringUtils.isNotBlank(device.getString(FieldName.Adview.os))) {
			setObj.append(FieldName.field_os, device.getString(FieldName.Adview.os));
		}
		// os_version
		if (StringUtils.isNotBlank(device.getString(FieldName.Adview.osv))) {
			setObj.append(FieldName.field_osVersion, device.getString(FieldName.Adview.osv));
		}

		// device_size 高x宽
		if (StringUtils.isNotBlank(device.getString(FieldName.Adview.sh))) {
			setObj.append(FieldName.field_deviceSize,
					device.getString(FieldName.Adview.sh) + "x" + device.getString(FieldName.Adview.sw));
		}
		// 性别
		JSONObject user = jsonObject.getJSONObject(FieldName.Adview.user);
		if (user != null && StringUtils.isNotBlank(user.getString(FieldName.Adview.gender))) {
			setObj.append(FieldName.field_gender, user.getString(FieldName.Adview.gender));
		}

		// builder.add(MongoSink.OP_SET, setObj);

		BasicDBObject addToSetObj = new BasicDBObject();

		// ip
		if (StringUtils.isNotBlank(device.getString(FieldName.Adview.ip))) {
			addToSetObj.append(FieldName.field_ipList, device.getString(FieldName.Adview.ip));
		}

		// 城市名称
		if (jsonObject.getJSONObject(FieldName.Adview.city) != null) {
			addToSetObj.append(FieldName.field_cityList,
					jsonObject.getJSONObject(FieldName.Adview.city).getString(FieldName.Adview.cityName));
		}

		// 追加的数据，geo
		if (device.getJSONObject(FieldName.Adview.geo) != null) {
			addToSetObj.append(FieldName.field_geoList,
					new BasicDBObject(FieldName.field_latitude,
							device.getJSONObject(FieldName.Adview.geo).getString(FieldName.Adview.lat)).append(
									FieldName.field_longitude,
									device.getJSONObject(FieldName.Adview.geo).getString(FieldName.Adview.lon)));
		}

		builder.add(MongoSink.OP_ADD_TO_SET, addToSetObj);

		// app

		JSONObject appJson = jsonObject.getJSONObject(FieldName.Adview.app);

		// 以包名为准
		if (StringUtils.isNotBlank(appJson.getString(FieldName.Adview.bundle))) {

			BasicDBObject appObj = new BasicDBObject();
			appObj.append(FieldName.field_packageName, appJson.getString(FieldName.Adview.bundle));
			if (StringUtils.isNotBlank(appJson.getString(FieldName.Adview.name))) {
				appObj.append(FieldName.field_appName, appJson.getString(FieldName.Adview.name));
			}

			// app分类
			if (appJson.getJSONArray(FieldName.Adview.cat) != null
					&& !appJson.getJSONArray(FieldName.Adview.cat).isEmpty()) {
				appObj.append(FieldName.field_appCategorys, appJson.getJSONArray(FieldName.Adview.cat).getString(0));
			}

			if (StringUtils.isNotBlank(jsonObject.getString(FieldName.bidRequestTime))) {
				appObj.append(FieldName.lastUseTime, jsonObject.getString(FieldName.bidRequestTime));
			}

			// mongo 不支持包含点. 的做key， 替换为_下划线
			setObj.append(FieldName.field_appList + appJson.getString(FieldName.Adview.bundle).replace(".", "_"),
					appObj);
		}

		if (addToSetObj.isEmpty()) {
			return null;
		}

		// app需要记录最后使用日期， 每次都更新
		builder.add(MongoSink.OP_SET, setObj);

		return builder.get();

	}

	public boolean check(JSONObject jsonObject) {
		boolean result = true;

		// response不处理， infoType = 11 的为竞价请求， 其他的不处理
		if (StringUtils.isBlank(jsonObject.getString(FieldName.Adview.infoType)) || !StringUtils
				.equals(InfoType.BID_REQUEST.getKey(), jsonObject.getString(FieldName.Adview.infoType))) {
			return false;
		}

		if (jsonObject.getJSONObject(FieldName.Adview.device) == null) {
			return false;
		}

		JSONObject device = jsonObject.getJSONObject(FieldName.Adview.device);

		// 两个设备id都为空
		if (StringUtils.isBlank(device.getString(FieldName.Adview.didsha1))
				&& StringUtils.isBlank(device.getString(FieldName.Adview.dpidsha1))) {
			return false;
		}

		return result;
	}

}
