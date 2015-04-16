package com.spikeify;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.async.IAsyncClient;

public class Scanner<T> {

	private final Class<T> type;
	private final IAerospikeClient synClient;
	private final IAsyncClient asyncClient;
	private final ClassConstructor classConstructor;
	private final RecordsCache recordsCache;

	public Scanner(Class<T> type, IAerospikeClient synClient, IAsyncClient asyncClient, ClassConstructor classConstructor, RecordsCache recordsCache) {
		this.type = type;
		this.synClient = synClient;
		this.asyncClient = asyncClient;
		this.classConstructor = classConstructor;
		this.recordsCache = recordsCache;
	}
}
