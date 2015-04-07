package com.spikeify;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.async.AsyncClient;

public class Updater<T> extends BaseCommand<T> implements Command<T>{

	public Updater(Class<T> type, AerospikeClient synCLient, AsyncClient asyncClient, ClassConstructor classConstructor) {
		super(type, synCLient, asyncClient, classConstructor);
	}

	@Override
	public T now() {

		Key key = checkKey();
		String useNamespace = getNamespace();
		String useSetName = getSetName();

//		mapper.getProperties(ob)
//
//		synCLient.put(policy, key, );

		return null;
	}
}
