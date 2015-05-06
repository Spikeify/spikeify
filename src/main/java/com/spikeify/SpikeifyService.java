package com.spikeify;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.async.AsyncClient;
import com.aerospike.client.async.AsyncClientPolicy;
import com.aerospike.client.async.IAsyncClient;
import com.aerospike.client.policy.ClientPolicy;

/**
 * This is a helper service that provides a Spikeify instance with a default single-cluster configuration.
 */
public class SpikeifyService {

	public static void globalConfig(String defaultNamespace, int port, String... urls) {
		Host[] hosts = new Host[urls.length];
		for (int i = 0; i < hosts.length; i++) {
			hosts[i] = new Host(urls[i], port);
		}
		globalConfig(defaultNamespace, hosts);
	}

	public static void globalConfig(String defaultNamespace, Host... hosts) {
		synClient = new AerospikeClient(null, hosts);
		asyncClient = new AsyncClient(null, hosts);
		SpikeifyService.defaultNamespace = defaultNamespace;
	}

	private static IAerospikeClient synClient;
	private static IAsyncClient asyncClient;
	public static String defaultNamespace;

	public static IAerospikeClient getClient(){
		return synClient;
	}

	/**
	 * A Spikeify with default global configuration. It uses no-arg constructors to instantiate classes.
	 *
	 * @return Spikeify instance
	 */
	public static Spikeify sfy() {

		if (synClient == null || asyncClient == null) {
			throw new SpikeifyError("Missing configuration: you must call SpikeifyService.globalConfig(..) once, before using SpikeifyService.sfy().");
		}

		return new SpikeifyImpl(synClient, asyncClient, new NoArgClassConstructor(), defaultNamespace);
	}

	/**
	 * A Spikeify that uses a mock AerospikeClient
	 *
	 * @return A mock Spikeify instance
	 */
	public static Spikeify mock(IAerospikeClient client) {
		return new SpikeifyImpl(client, null, new NoArgClassConstructor(), defaultNamespace);
	}

	/**
	 * Creates a new instance of Spikeify with given parameters.
	 * @param namespace
	 * @param port
	 * @param hosts
	 * @return Spikeify instance
	 */
	public static Spikeify instance(String namespace, int port, String... hosts) {
		Host[] hostsH = new Host[hosts.length];
		for (int i = 0; i < hosts.length; i++) {
			hostsH[i] = new Host(hosts[i], port);
		}
		return instance(namespace, hostsH);
	}

	/**
	 * Creates a new instance of Spikeify with given parameters.
	 * @param namespace
	 * @param hosts
	 * @return Spikeify instance
	 */
	public static Spikeify instance(String namespace, Host... hosts) {
		return new SpikeifyImpl<>(new AerospikeClient(new ClientPolicy(), hosts), new AsyncClient(new AsyncClientPolicy(), hosts), new NoArgClassConstructor(), namespace);
	}

}
