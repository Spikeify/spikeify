package com.spikeify;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.async.AsyncClient;

/**
 * This is a helper service that provides a Spikeify instance with a default single-cluster configuration.
 */
public class SpikeifyService {

	public static void globalConfig(String host, int port) {
		synClient = new AerospikeClient(host, port);
		asyncClient = new AsyncClient(host, port);
	}

	private static AerospikeClient synClient;
	private static AsyncClient asyncClient;

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

}
