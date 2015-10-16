package com.spikeify;

import com.aerospike.client.*;
import com.aerospike.client.async.AsyncClient;
import com.aerospike.client.async.AsyncClientPolicy;
import com.aerospike.client.async.IAsyncClient;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.InfoPolicy;
import com.aerospike.client.policy.Policy;
import com.spikeify.commands.InfoFetcher;

/**
 * This is a helper service that provides a Spikeify instance with a default single-cluster configuration.
 */
@SuppressWarnings({"SameParameterValue", "WeakerAccess"})
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

		checkDoubleSupport(synClient);

		SpikeifyService.defaultNamespace = defaultNamespace;
	}

	private static IAerospikeClient synClient;
	private static IAsyncClient asyncClient;
	public static String defaultNamespace;

	public static IAerospikeClient getClient() {
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
	 * @param client Native Aerospike client
	 * @return A mock Spikeify instance
	 */
	public static Spikeify mock(IAerospikeClient client) {
		return new SpikeifyImpl(client, null, new NoArgClassConstructor(), defaultNamespace);
	}

	/**
	 * Creates a new instance of Spikeify with given parameters.
	 *
	 * @param namespace Default namespace
	 * @param port      Default port
	 * @param hosts     Aerospike server hosts
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
	 *
	 * @param namespace Default namespace
	 * @param hosts     Aerospike server hosts
	 * @return Spikeify instance
	 */
	public static Spikeify instance(String namespace, Host... hosts) {
		AerospikeClient synClient = new AerospikeClient(new ClientPolicy(), hosts);
		AsyncClient asyncClient = new AsyncClient(new AsyncClientPolicy(), hosts);

		checkDoubleSupport(synClient);

		return new SpikeifyImpl<>(synClient, asyncClient, new NoArgClassConstructor(), namespace);
	}

	/**
	 * Registers entity of type and creates indexes annotated with @see(@Index) annotation
	 *
	 * @param clazz to be registered (Aerospike entity)
	 */
	public static void register(Class<?> clazz) {
		register(clazz, new Policy());
	}

	/**
	 * Registers entity of type and creates indexes annotated with @see(@Index) annotation
	 *
	 * @param classes list of classes to be registered (Aerospike entity)
	 */
	public static void register(Class<?>... classes) {

		for (Class item: classes) {
			register(item, new Policy());
		}
	}

	/**
	 * Registers entity of type and creates indexes annotated with @see(@Index) annotation
	 *
	 * @param clazz to be registered (Aerospike entity)
	 * @param policy usage policy
	 */
	public static void register(Class<?> clazz, Policy policy) {
		IndexingService.createIndex(sfy(), policy, clazz);
	}

	/**
	 * Checks if server supports saving double values. This is supported on servers higher or equal 3.6.0
	 * @param client Native Aerospike client
	 */
	public static void checkDoubleSupport(IAerospikeClient client){
		InfoFetcher.Build build = getServerBuild(client);
		// check that server build is >= 3.6.0
		if (build.major >= 3 && build.minor >= 6) {
			Value.UseDoubleType = true;  // enable server side support for double numbers
		}
	}

	/**
	 * Private helper method to get servers build number.
	 *
	 * @param client Native Aerospike client
	 * @return Server build version as InfoFetcher.Build
	 */
	public static InfoFetcher.Build getServerBuild(IAerospikeClient client) {
		Node[] nodes = client.getNodes();
		if (nodes == null || nodes.length == 0) {
			throw new IllegalStateException("No Aerospike nodes found.");
		}
		String build = Info.request(new InfoPolicy(), nodes[0], "build");
		String[] buildNumbers = build.split("\\.");
		return new InfoFetcher.Build(Integer.valueOf(buildNumbers[0]), Integer.valueOf(buildNumbers[1]), Integer.valueOf(buildNumbers[2]));
	}
}
