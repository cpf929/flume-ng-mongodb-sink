package org.apache.flume.sink.mongo.parse;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.sink.mongo.MongoSink;
import org.apache.flume.sink.mongo.constant.FieldName;

import com.alibaba.fastjson.JSONObject;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;

/**
 * 处理tanx平台的日志
 * 
 * @author chenpf
 *
 */
public class TanxEventHandler implements EventHandler {

	public DBObject buildDbObject(JSONObject jsonObject) {

		if (!check(jsonObject)) {
			return null;
		}

		BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();
		JSONObject mobile = jsonObject.getJSONObject(FieldName.Tanx.mobile);
		JSONObject device = mobile.getJSONObject(FieldName.Tanx.device);
		String deviceId = device.getString(FieldName.Tanx.device_id);

		// 新增， 不更新的
		BasicDBObject setOnInsertObj = new BasicDBObject(FieldName.field_deviceId, deviceId)
				.append(FieldName.field_deviceIdMd5, DigestUtils.md5Hex(deviceId))
				.append(FieldName.field_deviceIdSha1, DigestUtils.sha1Hex(deviceId));

		// brand
		if (StringUtils.isNotBlank(device.getString(FieldName.Tanx.brand))) {
			setOnInsertObj.append(FieldName.field_brand, device.getString(FieldName.Tanx.brand));
		}
		// model
		if (StringUtils.isNotBlank(device.getString(FieldName.Tanx.model))) {
			setOnInsertObj.append(FieldName.field_model, device.getString(FieldName.Tanx.model));
		}
		// platform
		if (StringUtils.isNotBlank(device.getString(FieldName.Tanx.platform))) {
			setOnInsertObj.append(FieldName.field_platform, device.getString(FieldName.Tanx.platform));
		}
		// os
		if (StringUtils.isNotBlank(device.getString(FieldName.Tanx.os))) {
			setOnInsertObj.append(FieldName.field_os, device.getString(FieldName.Tanx.os));
		}
		// os_version
		if (StringUtils.isNotBlank(device.getString(FieldName.Tanx.os_version))) {
			setOnInsertObj.append(FieldName.field_osVersion, device.getString(FieldName.Tanx.os_version));
		}
		// mac
		String mac = device.getString(FieldName.Tanx.mac);
		if (StringUtils.isNotBlank(mac)) {
			setOnInsertObj.append(FieldName.field_mac, mac).append(FieldName.field_macMd5, DigestUtils.md5Hex(mac))
					.append(FieldName.field_macSha1, DigestUtils.sha1Hex(mac));
		}
		// device_size
		if (StringUtils.isNotBlank(device.getString(FieldName.Tanx.device_size))) {
			setOnInsertObj.append(FieldName.field_deviceSize, device.getString(FieldName.Tanx.device_size));
		}

		builder.add(MongoSink.OP_SET_ON_INSERT, setOnInsertObj);

		BasicDBObject addToSetObj = new BasicDBObject();

		// ip
		if (StringUtils.isNotBlank(jsonObject.getString(FieldName.Tanx.ip))) {
			addToSetObj.append(FieldName.field_ipList, jsonObject.getString(FieldName.Tanx.ip));
		}

		// 追加的数据，geo
		if (StringUtils.isNotBlank(device.getString(FieldName.Tanx.latitude))
				&& StringUtils.isNotBlank(device.getString(FieldName.Tanx.longitude))) {
			addToSetObj.append(FieldName.field_geoList,
					new BasicDBObject(FieldName.field_latitude, device.getString(FieldName.Tanx.latitude))
							.append(FieldName.field_longitude, device.getString(FieldName.Tanx.longitude)));
		}

		// app
		if (StringUtils.isNotBlank(mobile.getString(FieldName.Tanx.package_name))) {
			BasicDBObject appObj = new BasicDBObject();
			appObj.append(FieldName.field_packageName, mobile.getString(FieldName.Tanx.package_name));
			if (StringUtils.isNotBlank(mobile.getString(FieldName.Tanx.app_name))) {
				appObj.append(FieldName.field_appName, mobile.getString(FieldName.Tanx.app_name));
			}

			// app分类
			if (mobile.getJSONArray(FieldName.Tanx.app_categories) != null
					&& !mobile.getJSONArray(FieldName.Tanx.app_categories).isEmpty()) {
				appObj.append(FieldName.field_appCategorys, mobile.getJSONArray(FieldName.Tanx.app_categories)
						.getJSONObject(0).getString(FieldName.Tanx.id));
			}

			addToSetObj.append(FieldName.field_appList, appObj);
		}

		builder.add(MongoSink.OP_ADD_TO_SET, addToSetObj);

		return builder.get();
	}

	public boolean check(JSONObject jsonObject) {

		boolean result = true;

		// response不处理
		if (StringUtils.isNotBlank(jsonObject.getString(FieldName.bidResponseTime))) {
			return false;
		}

		if (jsonObject.getJSONObject(FieldName.Tanx.mobile) == null) {
			return false;
		}

		JSONObject device = jsonObject.getJSONObject(FieldName.Tanx.mobile).getJSONObject(FieldName.Tanx.device);

		if (device == null || StringUtils.isBlank(device.getString(FieldName.Tanx.device_id))) {
			return false;
		}

		return result;
	}

}
