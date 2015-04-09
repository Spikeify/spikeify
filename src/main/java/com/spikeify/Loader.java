package com.spikeify;

import com.aerospike.client.*;
import com.aerospike.client.async.AsyncClient;
import com.aerospike.client.async.IAsyncClient;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;

import java.util.Map;

public class Loader<T>{

	protected String namespace;
	protected String setName;
	protected String stringKey;
	protected Long longKey;
	protected IAerospikeClient synClient;
	protected IAsyncClient asyncClient;
	protected ClassConstructor classConstructor;
	private RecordsCache recordsCache;
	protected Policy policy;
	protected ClassMapper<T> mapper;
	protected Class<T> type;

	public Loader(Class<T> type, IAerospikeClient synClient, IAsyncClient asyncClient, ClassConstructor classConstructor, RecordsCache recordsCache) {
		this.synClient = synClient;
		this.asyncClient = asyncClient;
		this.classConstructor = classConstructor;
		this.recordsCache = recordsCache;
		this.policy = new Policy();
		this.policy.sendKey = true;
		this.mapper = MapperService.getMapper(type);
		this.type = type;
	}

	public Loader<T> namespace(String namespace) {
		this.namespace = namespace;
		return this;
	}

	public Loader<T> set(String setName) {
		this.setName = setName;
		return this;
	}

	public Loader<T> key(String key) {
		this.stringKey = key;
		this.longKey = null;
		return this;
	}

	public Loader<T> key(long key) {
		this.longKey = key;
		this.stringKey = null;
		return this;
	}

	public Loader<T> key(Key key) {
		this.namespace = key.namespace;
		this.setName = key.setName;
		Value userKey = key.userKey;
		if (userKey instanceof Value.StringValue) {
			this.stringKey = ((Value.StringValue) userKey).toString();
		} else if (userKey instanceof Value.LongValue) {
			this.longKey = ((Value.LongValue) userKey).toLong();
		} else {
			throw  new IllegalStateException("Spikeify only supports Keys created from String and Long.");
		}
		return this;
	}

	public Loader<T> policy(Policy policy) {
		this.policy = policy;
		this.policy.sendKey = true;
		return this;
	}

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
		String useNamespace = namespace != null ? namespace : mapper.getNamespace();
		if (useNamespace == null) {
			throw new IllegalStateException("Namespace not set.");
		}
		return useNamespace;
	}

	protected String getSetName() {
		return setName != null ? setName : mapper.getSetName();
	}

	public T now() {

		Key key = checkKey();
		String useNamespace = getNamespace();
		String useSetName = getSetName();

		Record record = synClient.get(policy, key);
		T object = classConstructor.construct(type);

		// save rew records into cache - used later for differential updating
		recordsCache.insert(key, record.bins);

		mapper.setMetaFieldValues(object, useNamespace, useSetName, record.generation, record.expiration);
		mapper.setFieldValues(object, record.bins);

		return object;
	}

}
