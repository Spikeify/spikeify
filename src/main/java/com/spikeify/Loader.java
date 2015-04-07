package com.spikeify;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.async.AsyncClient;

public class Loader<T> extends BaseCommand<T> implements Command<T> {

	public Loader(Class<T> type, AerospikeClient synCLient, AsyncClient asyncClient, ClassConstructor classConstructor) {
		super(type, synCLient, asyncClient, classConstructor);
	}

	public T now() {

		Key key = checkKey();
		String useNamespace = getNamespace();
		String useSetName = getSetName();

		Record record = synCLient.get(policy, key);
		T object = classConstructor.construct(type);

		mapper.setMetaFieldValues(object, useNamespace, useSetName, record.generation, record.expiration);

		mapper.setFieldValues(object, record.bins);

		return object;
	}

}
