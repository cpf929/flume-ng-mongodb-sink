package org.apache.flume.sink.mongo.json;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.alibaba.fastjson.JSON;

/**
 * 存储到mongodb中的完整字符串 ， 对应一个document
 * 
 * @author chenpf
 *
 */
public class DmpInfo {

	/** 设备号， imei或者idfa */
	private String deviceId;
	/** bes设备号使用md5加密 */
	private String deviceIdMd5;
	/** adview设备号使用sha1加密 */
	private String deviceIdSha1;

	/** mac地址 */
	private String mac;
	/** adview的mac使用sha1加密 */
	private String macSha1;
	/** bes的mac使用md5加密 */
	private String macMd5;

	/** 品牌 */
	private String brand;
	/** 型号 */
	private String model;
	/** 系统 */
	private String os;
	/** 平台 */
	private String platform;
	/** 系统版本号 */
	private String osVersion;
	/** 分辨率 */
	private String deviceSize;

	/** 历史ip信息 */
	private List<String> ipList;
	/** 该设备的app信息 */
	private List<AppInfo> appList;
	/** 该设备的geo信息 */
	private List<GeoInfo> geoList;

	public String getDeviceId() {
		return deviceId;
	}

	public void setDeviceId(String deviceId) {
		this.deviceId = deviceId;
	}

	public String getDeviceIdMd5() {
		return deviceIdMd5;
	}

	public void setDeviceIdMd5(String deviceIdMd5) {
		this.deviceIdMd5 = deviceIdMd5;
	}

	public String getDeviceIdSha1() {
		return deviceIdSha1;
	}

	public void setDeviceIdSha1(String deviceIdSha1) {
		this.deviceIdSha1 = deviceIdSha1;
	}

	public String getMac() {
		return mac;
	}

	public void setMac(String mac) {
		this.mac = mac;
	}

	public String getMacSha1() {
		return macSha1;
	}

	public void setMacSha1(String macSha1) {
		this.macSha1 = macSha1;
	}

	public String getMacMd5() {
		return macMd5;
	}

	public void setMacMd5(String macMd5) {
		this.macMd5 = macMd5;
	}

	public String getBrand() {
		return brand;
	}

	public void setBrand(String brand) {
		this.brand = brand;
	}

	public String getModel() {
		return model;
	}

	public void setModel(String model) {
		this.model = model;
	}

	public String getOs() {
		return os;
	}

	public void setOs(String os) {
		this.os = os;
	}

	public String getPlatform() {
		return platform;
	}

	public void setPlatform(String platform) {
		this.platform = platform;
	}

	public String getOsVersion() {
		return osVersion;
	}

	public void setOsVersion(String osVersion) {
		this.osVersion = osVersion;
	}

	public String getDeviceSize() {
		return deviceSize;
	}

	public void setDeviceSize(String deviceSize) {
		this.deviceSize = deviceSize;
	}

	public List<String> getIpList() {
		return ipList;
	}

	public void setIpList(List<String> ipList) {
		this.ipList = ipList;
	}

	public List<AppInfo> getAppList() {
		return appList;
	}

	public void setAppList(List<AppInfo> appList) {
		this.appList = appList;
	}

	public List<GeoInfo> getGeoList() {
		return geoList;
	}

	public void setGeoList(List<GeoInfo> geoList) {
		this.geoList = geoList;
	}

	@Override
	public String toString() {
		return "DmpInfo [deviceId=" + deviceId + ", deviceIdMd5=" + deviceIdMd5 + ", deviceIdSha1=" + deviceIdSha1
				+ ", mac=" + mac + ", macSha1=" + macSha1 + ", macMd5=" + macMd5 + ", brand=" + brand + ", model="
				+ model + ", os=" + os + ", platform=" + platform + ", osVersion=" + osVersion + ", deviceSize="
				+ deviceSize + ", ipList=" + ipList + ", appList=" + appList + ", geoList=" + geoList + "]";
	}
	
	
	
	public static void main(String[] args) {
		DmpInfo dmpInfo = new DmpInfo();
		dmpInfo.setBrand("huawei");
		dmpInfo.setDeviceId("1234567890");
		dmpInfo.setDeviceIdMd5("aqe2e2e2eed34343reefefe");
		dmpInfo.setDeviceIdSha1("adswrvdvd44re3rgbe324rfvf");
		dmpInfo.setDeviceSize("1024x768");
		dmpInfo.setIpList(Arrays.asList("192.168.1.1", "192.168.1.2"));
		dmpInfo.setMac("10:23:1a:23:13");
		dmpInfo.setMacMd5("wef3err3243434");
		dmpInfo.setMacSha1("1234r23erf323erf234ere");
		dmpInfo.setModel("p6");
		dmpInfo.setOs("android");
		dmpInfo.setOsVersion("4.4.4");
		dmpInfo.setPlatform("android");
		
		List<AppInfo> appList = new ArrayList<AppInfo>();
		
		AppInfo appInfo = new AppInfo();
		appInfo.setAppCategorys(Arrays.asList("101021", "103123"));
		appInfo.setAppName("微信");
		appInfo.setPackageName("com.tencent");
		appInfo.setVersion("6.0");
		appList.add(appInfo);
		
		dmpInfo.setAppList(appList);
		
		List<GeoInfo> geoList = new ArrayList<GeoInfo>();
		GeoInfo geoInfo = new GeoInfo();
		geoInfo.setLatitude("28.134");
		geoInfo.setLongitude("123.123");
		geoList.add(geoInfo);
		
		dmpInfo.setGeoList(geoList);
		
		
		
		System.out.println(JSON.toJSONString(dmpInfo));
	}

}
