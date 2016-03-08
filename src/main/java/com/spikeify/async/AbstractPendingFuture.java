package com.spikeify.async;

import com.aerospike.client.Key;

import java.util.concurrent.*;

public abstract class AbstractPendingFuture<K> implements Future<K> {

	private Exception exception;
	private K result;
	private CountDownLatch latch;


	public abstract K prepareResult(Key key);


	private volatile boolean haveResult;

	private boolean prepareForWait() {
		synchronized (this) {
			if (this.haveResult) {
				return false;
			} else {
				if (this.latch == null) {
					this.latch = new CountDownLatch(1);
				}
				return true;
			}
		}
	}

	void setResult(K var1) {
		synchronized (this) {
			if (!this.haveResult) {
				this.result = var1;
				this.haveResult = true;

				if (this.latch != null) {
					this.latch.countDown();
				}

			}
		}
	}

	void setFailure(Exception exc) {
		synchronized (this) {
			if (!this.haveResult) {
				this.exception = exc;
				this.haveResult = true;

				if (this.latch != null) {
					this.latch.countDown();
				}

			}
		}
	}

	public K get() throws ExecutionException, InterruptedException {
		if (!this.haveResult) {
			boolean preparedToWait = this.prepareForWait();
			if (preparedToWait) {
				this.latch.await();
			}
		}

		if (this.exception != null) {
			throw new ExecutionException(this.exception);
		} else {
			return this.result;
		}
	}

	public K get(long timeout, TimeUnit timeUnit) throws ExecutionException, InterruptedException, TimeoutException {
		if (!this.haveResult) {
			boolean var4 = this.prepareForWait();
			if (var4 && !this.latch.await(timeout, timeUnit)) {
				throw new TimeoutException();
			}
		}

		if (this.exception != null) {
			throw new ExecutionException(this.exception);
		} else {
			return this.result;
		}
	}

	public boolean isCancelled() {
		return this.exception != null;
	}

	public boolean isDone() {
		return this.haveResult;
	}

	public boolean cancel(boolean mayInterruptIfRunning) {
		synchronized (this) {
			if (this.haveResult) {
				return false;
			}
			this.haveResult = true;
		}

		if (this.latch != null) {
			this.latch.countDown();
		}

		return true;
	}
}
