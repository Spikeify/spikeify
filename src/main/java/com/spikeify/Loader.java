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
	private AerospikeClient synCLient;
	private AsyncClient asyncClient;
	private ClassConstructor classConstructor;
	private Policy readPolicy;
	private ClassMapper<T> mapper;
	private Class<T> type;

	public Loader(AerospikeClient synCLient, AsyncClient asyncClient, ClassConstructor classConstructor) {
		this.synCLient = synCLient;
		this.asyncClient = asyncClient;
		this.classConstructor = classConstructor;
	}

	public Loader type(Class<T> type) {
		this.type = type;
		mapper = MapperService.getMapper(type);
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
		String useNamespace = namespace != null ? namespace : mapper.getNamespace();
		String useSetName = setName != null ? setName : mapper.getSetName();
		if (stringKey != null) {
			key = new Key(useNamespace, useSetName, stringKey);
		} else if (longKey != null) {
			key = new Key(useNamespace, useSetName, longKey);
		} else {
			throw new IllegalStateException("Error: missing parameter 'key'");
		}
		Record record = synCLient.get(readPolicy, key);
		T object = classConstructor.construct(type);

		mapper.setFieldValues(object, record.bins);

		return object;
	}

}
