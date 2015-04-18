package com.spikeify;

import com.aerospike.client.Key;

public class Ref<T> {

	private Class<T> type;
	private Key key;
	private Long longUserKey;
	private String stringUserKey;

	public Ref(Class<T> type, Key key) {
		this.type = type;
		this.key = key;
	}

	public Ref(Class<T> type, String userKey) {

	}

	public Ref(Class<T> type, Long userKey) {
	}

	public static <T> Ref<T> create(Class<T> type, Key key) {
		return new Ref<>(type, key);
	}

	public static <T> Ref<T> create(Class<T> type, String userKey) {
		return new Ref<>(type, userKey);
	}

	public static <T> Ref<T> create(Class<T> type, Long userKey) {
		return new Ref<>(type, userKey);
	}
}
