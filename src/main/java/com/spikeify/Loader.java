package com.spikeify;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.async.AsyncClient;
import com.aerospike.client.policy.Policy;

public class Loader<T> implements Command<T> {

	private String namespace;
	private String setName;
	private String stringKey;
	private Long longKey;
	private Class<T> type;
	private AerospikeClient synCLient;
	private AsyncClient asyncClient;
	private ClassConstructor classConstructor;
	private Policy readPolicy;

	public Loader(AerospikeClient synCLient, AsyncClient asyncClient, ClassConstructor classConstructor) {
		this.synCLient = synCLient;
		this.asyncClient = asyncClient;
		this.classConstructor = classConstructor;
	}

	public Loader type(Class<T> type) {
		this.type = type;
		return this;
	}

	public Loader namespace(String namespace) {
		this.namespace = namespace;
		return this;
	}

	public Loader set(String setName) {
		this.setName = setName;
		return this;
	}

	public Loader key(String key) {
		this.stringKey = key;
		this.longKey = null;
		return this;
	}

	public Loader key(long key) {
		this.longKey = key;
		this.stringKey = null;
		return this;
	}

	public Loader policy(Policy readPolicy) {
		this.readPolicy = readPolicy;
		return this;
	}

	public T now() {

		Key key;
		if (stringKey != null) {
			key = new Key(namespace, setName, stringKey);
		} else if (longKey != null) {
			key = new Key(namespace, setName, longKey);
		} else {
			throw new IllegalStateException("Error: missing parameter 'key'");
		}
		Record record = synCLient.get(readPolicy, key);
		ClassMapper<T> mapper = MapperService.getMapper(type);
		T object  = classConstructor.construct(type);

		mapper.setFieldValues(object, record.bins);

		return object;
	}

}
