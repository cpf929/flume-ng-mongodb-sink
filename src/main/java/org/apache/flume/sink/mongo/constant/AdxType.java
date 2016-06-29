package org.apache.flume.sink.mongo.constant;

public enum AdxType {

	ALL("all", 0),

	TANX("tanx", 1),

	BES("bes", 2),

	ADVIEW("adview", 3),

	;

	private String key;
	private int value;

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public int getValue() {
		return value;
	}

	public void setValue(int value) {
		this.value = value;
	}

	private AdxType(String key, int value) {
		this.key = key;
		this.value = value;
	}
}
