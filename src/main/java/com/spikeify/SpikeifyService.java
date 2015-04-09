package com.spikeify;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.async.AsyncClient;
import com.aerospike.client.async.IAsyncClient;
import com.spikeify.mock.AerospikeClientMock;

/**
 * This is a helper service that provides a Spikeify instance with a default single-cluster configuration.
 */
public class SpikeifyService {

	public static void globalConfig(String host, int port) {
		synClient = new AerospikeClient(host, port);
		asyncClient = new AsyncClient(host, port);
	}

	private static IAerospikeClient synClient;
	private static IAsyncClient asyncClient;

	/**
	 * A Spikeify with default global configuration. It uses no-arg constructors to instantiate classes.
	 *
	 * @return Spikeify instance
	 */
	public static Spikeify sfy() {

		if (synClient == null || asyncClient == null) {
			throw new IllegalStateException("Missing configuration: you must call SpikeifyService.globalConfig(..) once, before using SpikeifyService.sfy().");
		}

		return new SpikeifyImpl(synClient, asyncClient, new NoArgClassConstructor());
	}

	/**
	 * A Spikeify that uses a mock AerospikeClient
	 *
	 * @return Spikeify instance
	 */
	public static Spikeify mock(IAerospikeClient client) {

		return new SpikeifyImpl(client, null, new NoArgClassConstructor());
	}

}
