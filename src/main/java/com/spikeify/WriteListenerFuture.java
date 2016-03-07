package com.spikeify;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.listener.WriteListener;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class WriteListenerFuture<K> implements Future<K>, WriteListener {

	private boolean done = false;
	private K result = null;

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		return false;
	}

	@Override
	public boolean isCancelled() {
		return false;
	}

	@Override
	public boolean isDone() {
		return done;
	}

	@Override
	public K get() throws InterruptedException, ExecutionException {
		return result;
	}

	@Override
	public K get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		return null;
	}

	@Override
	public void onSuccess(Key key) {

	}

	@Override
	public void onFailure(AerospikeException exception) {

	}
}
