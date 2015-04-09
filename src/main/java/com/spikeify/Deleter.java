package com.spikeify;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.async.AsyncClient;

import java.util.Map;

public class Deleter<T> {

	public Deleter(AerospikeClient synClient, AsyncClient asyncClient, RecordsCache recordsCache) {
		this.synClient = synClient;
		this.asyncClient = asyncClient;
		this.recordsCache = recordsCache;
	}

	private String namespace;
	private String setName;
	private String stringKey;
	private Long longKey;
	private AerospikeClient synClient;
	private AsyncClient asyncClient;
	private RecordsCache recordsCache;

	public Deleter<T> namespace(String namespace) {
		this.namespace = namespace;
		return this;
	}

	public Deleter<T> set(String setName) {
		this.setName = setName;
		return this;
	}

	public Deleter<T> key(String key) {
		this.stringKey = key;
		this.longKey = null;
		return this;
	}

	public Deleter<T> key(long key) {
		this.longKey = key;
		this.stringKey = null;
		return this;
	}

	public Deleter<T> key(Key key) {
		this.namespace = key.namespace;
		this.setName = key.setName;
		Value userKey = key.userKey;
		if (userKey instanceof Value.StringValue) {
			this.stringKey = ((Value.StringValue) userKey).toString();
		} else if (userKey instanceof Value.LongValue) {
			this.longKey = ((Value.LongValue) userKey).toLong();
		} else {
			throw new IllegalStateException("Spikeify only supports Keys created from String and Long.");
		}
		return this;
	}

//	public Deleter<T> entity(T object) {
//		ClassMapper<?> mapper = MapperService.getMapper(object.getClass());
//		this.namespace = mapper.getNamespace();
//		this.setName = mapper.getSetName();
//		Object userKey = mapper.getUserKey(object);
//		if (userKey == null) {
//			throw new IllegalStateException("Method Deleter.entity(object) requires entities to have");
//		}
//		if (userKey instanceof Value.StringValue) {
//			this.stringKey = ((Value.StringValue) userKey).toString();
//		} else if (userKey instanceof Value.LongValue) {
//			this.longKey = ((Value.LongValue) userKey).toLong();
//		} else {
//			throw new IllegalStateException("Spikeify only supports Keys created from String and Long.");
//		}
//	}

	protected Key checkKey() {

		String useNamespace = getNamespace();
		String useSetName = getSetName();

		Key key;
		if (stringKey != null) {
			key = new Key(useNamespace, useSetName, stringKey);
		} else if (longKey != null) {
			key = new Key(useNamespace, useSetName, longKey);
		} else {
			throw new IllegalStateException("Error: missing parameter 'key'");
		}
		return key;
	}


	protected String getNamespace() {
		if (namespace == null) {
			throw new IllegalStateException("Namespace not set.");
		}
		return namespace;
	}

	protected String getSetName() {
		return setName != null ? setName : null; //mapper.getSetName();
	}

	/**
	 * Executes the delete command immediately.
	 *
	 * @return whether record existed on server before deletion
	 */
	public boolean now() {
		Key key = checkKey();
		recordsCache.remove(key);
		return synClient.delete(null, key);
	}

}
