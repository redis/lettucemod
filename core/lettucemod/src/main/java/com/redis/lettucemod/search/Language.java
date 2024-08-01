package com.redis.lettucemod.search;

public enum Language {

	ARABIC("Arabic"), CHINESE("Chinese"), DANISH("Danish"), DUTCH("Dutch"), ENGLISH("English"), FINNISH("Finnish"),
	FRENCH("French"), GERMAN("German"), HUNGARIAN("Hungarian"), ITALIAN("Italian"), NORWEGIAN("Norwegian"),
	PORTUGUESE("Portuguese"), ROMANIAN("Romanian"), RUSSIAN("Russian"), SPANISH("Spanish"), SWEDISH("Swedish"),
	TAMIL("Tamil"), TURKISH("Turkish");

	private final String id;

	private Language(String id) {
		this.id = id;
	}

	public String getId() {
		return id;
	}

}
