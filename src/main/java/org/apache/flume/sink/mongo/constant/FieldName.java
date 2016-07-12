package org.apache.flume.sink.mongo.constant;

public abstract class FieldName {

	public static final String field_brand = "brand";
	public static final String field_deviceId = "deviceId";
	public static final String field_deviceIdMd5 = "deviceIdMd5";
	public static final String field_deviceIdSha1 = "deviceIdSha1";
	public static final String field_deviceSize = "deviceSize";
	public static final String field_geoList = "geoList";
	public static final String field_latitude = "latitude";
	public static final String field_longitude = "longitude";
	public static final String field_ipList = "ipList";
	public static final String field_cityList = "cityList";
	public static final String field_mac = "mac";
	public static final String field_macMd5 = "macMd5";
	public static final String field_macSha1 = "macSha1";
	public static final String field_model = "model";
	public static final String field_os = "os";
	public static final String field_osVersion = "osVersion";
	public static final String field_platform = "platform";
	public static final String field_gender = "gender";
	public static final String field_age = "age";
	public static final String field_appList = "appList.";
	public static final String field_appCategorys = "appCategorys";
	public static final String field_appName = "appName";
	public static final String lastUseTime = "lastUseTime";
	public static final String field_packageName = "packageName";
	public static final String field_version = "version";

	public static final String bidRequestTime = "bidRequestTime";

	public static final String bidResponseTime = "bidResponseTime";

	public static class Tanx {
		public static final String ip = "ip";
		public static final String mobile = "mobile";
		public static final String package_name = "package_name";
		public static final String device = "device";
		public static final String platform = "platform";
		public static final String os = "os";
		public static final String device_id = "device_id";
		public static final String brand = "brand";
		public static final String model = "model";
		public static final String os_version = "os_version";
		public static final String mac = "mac";
		public static final String app_name = "app_name";
		public static final String app_categories = "app_categories";
		public static final String device_size = "device_size";
		public static final String longitude = "longitude";
		public static final String latitude = "latitude";
		public static final String id = "id";
		public static final String city = "city";

	}
	
	public static class Adview {
		public static final String infoType = "infoType";
		//设备信息
		public static final String device = "device";
		public static final String didsha1 = "didsha1";
		public static final String dpidsha1 = "dpidsha1";
		public static final String os = "os";
		public static final String make = "make";
		public static final String model = "model";
		public static final String osv = "osv";
		public static final String sh = "sh";
		public static final String sw = "sw";
		public static final String geo = "geo";
		public static final String lat = "lat";
		public static final String lon = "lon";
		public static final String ip = "ip";
		
		//app信息
		public static final String app = "app";
		public static final String bundle = "bundle";
		public static final String cat = "cat";
		public static final String name = "name";
		public static final String ver = "ver";
		
		public static final String user = "user";
		public static final String gender = "gender";
		
		
		public static final String city = "city";
		public static final String cityName = "cityName";
		
		
		
	}

}
