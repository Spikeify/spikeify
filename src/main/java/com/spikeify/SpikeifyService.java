package com.spikeify;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.async.AsyncClient;

/**
 * This is a helper service, that provides Spikeify with a default configuration.
 *
 */
public class SpikeifyService {

	public static void globalConfig(String host, int port){
		synClient = new AerospikeClient(host, port);
		asyncClient = new AsyncClient(host, port);
	}

	private static AerospikeClient synClient;
	private static AsyncClient asyncClient;

	/**
	 * A Spikeify with default configuration.
	 * @return
	 */
	public static Spikeify sfy(){
		return new SpikeifyImpl();
	}

}
