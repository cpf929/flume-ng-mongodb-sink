package org.apache.flume.sink.mongo.json;

import java.util.List;

/**
 * 使用过的app信息
 * 
 * @author chenpf
 *
 */
public class AppInfo {

	private String appName;
	private String packageName;
	private String version;
	private List<String> appCategorys;
	private String lastUseTime;

	public String getAppName() {
		return appName;
	}

	public void setAppName(String appName) {
		this.appName = appName;
	}

	public String getPackageName() {
		return packageName;
	}

	public void setPackageName(String packageName) {
		this.packageName = packageName;
	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

	public List<String> getAppCategorys() {
		return appCategorys;
	}

	public void setAppCategorys(List<String> appCategorys) {
		this.appCategorys = appCategorys;
	}

	public String getLastUseTime() {
		return lastUseTime;
	}

	public void setLastUseTime(String lastUseTime) {
		this.lastUseTime = lastUseTime;
	}

	@Override
	public String toString() {
		return "AppInfo [appName=" + appName + ", packageName=" + packageName + ", version=" + version
				+ ", appCategorys=" + appCategorys + ", lastUseTime=" + lastUseTime + "]";
	}

}
