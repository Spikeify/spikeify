package com.spikeify.async;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.listener.WriteListener;

public abstract class WriteListenerFuture<K> extends AbstractPendingFuture<K> implements WriteListener {

	@Override
	public synchronized void onSuccess(Key key) {
		K res = prepareResult(key);
		setResult(res);
	}

	@Override
	public void onFailure(AerospikeException e) {
		setFailure(e);
	}

}
