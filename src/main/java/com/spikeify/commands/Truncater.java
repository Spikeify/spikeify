package com.spikeify.commands;

import com.aerospike.client.*;
import com.spikeify.Spikeify;

/**
 * Class implementing truncate commands. This class is not intended to be instantiated by user.
 * Instead use {@link Spikeify#truncateSet(String, String)} or {@link Spikeify#truncateNamespace(String)} methods.
 */
public class Truncater {

	public static void truncateSet(String namespace, String setName, final IAerospikeClient client) {
		client.scanAll(null, namespace, setName, new ScanCallback() {
			@Override
			public void scanCallback(Key key, Record record) throws AerospikeException {
				client.delete(null, key);
			}
		});
	}


	public static void truncateNamespace(String namespace, final IAerospikeClient client) {
		client.scanAll(null, namespace, null, new ScanCallback() {
			@Override
			public void scanCallback(Key key, Record record) throws AerospikeException {
				client.delete(null, key);
			}
		});
	}
}
