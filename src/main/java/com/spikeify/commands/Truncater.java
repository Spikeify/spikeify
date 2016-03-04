package com.spikeify.commands;

import com.aerospike.client.*;
import com.aerospike.client.async.IAsyncClient;
import com.aerospike.client.policy.ScanPolicy;
import com.spikeify.Spikeify;

import java.util.Map;

/**
 * Class implementing truncate commands. This class is not intended to be instantiated by user.
 * Instead use {@link Spikeify#truncateSet(String, String)} or {@link Spikeify#truncateNamespace(String)} methods.
 */
public class Truncater {

	public static void truncateSet(String namespace, String setName, final IAerospikeClient client) {

		ScanPolicy scanPolicy = new ScanPolicy();
		scanPolicy.concurrentNodes = true;
		scanPolicy.includeBinData = false;

		client.scanAll(scanPolicy, namespace, setName, new ScanCallback() {
			@Override
			public void scanCallback(Key key, Record record) throws AerospikeException {
				client.delete(null, key);
			}
		});
	}


	public static void truncateNamespace(String namespace, final IAerospikeClient client) {

		ScanPolicy scanPolicy = new ScanPolicy();
		scanPolicy.concurrentNodes = true;
		scanPolicy.includeBinData = false;

		client.scanAll(scanPolicy, namespace, null, new ScanCallback() {
			@Override
			public void scanCallback(Key key, Record record) throws AerospikeException {
				client.delete(null, key);
			}
		});
	}


	/**
	 * Drops all indexes in namespace
	 *
	 * @param namespace
	 * @param client
	 */
	public static void dropNamespaceIndexes(String namespace, final IAsyncClient client) {

		Map<String, InfoFetcher.IndexInfo> namespaceIndexes = new InfoFetcher(client).getIndexes(namespace);
		for (InfoFetcher.IndexInfo index : namespaceIndexes.values()) {
			client.dropIndex(null, namespace, index.setName, index.name);
		}
	}

	/**
	 * Drops all indexes in set
	 *
	 * @param namespace
	 * @param setName
	 * @param client
	 */
	public static void dropSetIndexes(String namespace, String setName, final IAsyncClient client) {

		Map<String, InfoFetcher.IndexInfo> namespaceIndexes = new InfoFetcher(client).getIndexes(namespace, setName);
		for (InfoFetcher.IndexInfo index : namespaceIndexes.values()) {
			client.dropIndex(null, namespace, index.setName, index.name);
		}
	}
}
