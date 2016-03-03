package com.spikeify;

import com.aerospike.client.AerospikeClient;
import org.junit.After;
import org.junit.Before;

/**
 * Base test ... holding namespace and other data
 */
public class SpikeifyTest {

	public String namespace = "test";
	private boolean dropIndexesOnCleanup = true;

	Spikeify sfy;
	AerospikeClient client;

	public SpikeifyTest() {
		dropIndexesOnCleanup = true;
	}

	public SpikeifyTest(boolean dropIndexes) {
		dropIndexesOnCleanup = dropIndexes;
	}

	@Before
	public void dbSetup() {

		SpikeifyService.globalConfig(namespace, 3000, "localhost");
		sfy = SpikeifyService.sfy();

		client = new AerospikeClient("localhost", 3000);

		/*if (dropIndexesOnCleanup) {
			sfy.dropIndexesInNamespace(namespace);
		} */
		sfy.truncateNamespace(namespace);
	}

	@After
	public void dbCleanup() {
		sfy.truncateNamespace(namespace);

		/*if (dropIndexesOnCleanup) {
			sfy.dropIndexesInNamespace(namespace);
		}*/
	}
}
