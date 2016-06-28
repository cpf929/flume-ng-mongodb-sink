package org.apache.flume.sink.mongo.json;

/**
 * geo信息， 经纬度
 * 
 * @author chenpf
 *
 */
public class GeoInfo {

	/** 经度 */
	private String longitude;
	private String latitude;

	public String getLongitude() {
		return longitude;
	}

	public void setLongitude(String longitude) {
		this.longitude = longitude;
	}

	public String getLatitude() {
		return latitude;
	}

	public void setLatitude(String latitude) {
		this.latitude = latitude;
	}

	@Override
	public String toString() {
		return "GeoInfo [longitude=" + longitude + ", latitude=" + latitude + "]";
	}

}
