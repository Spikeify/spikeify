package com.spikeify;

import com.aerospike.client.*;
import com.spikeify.entity.EntityOne;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class InfoTest {

	private String namespace = "test";
	private List<String> setNames = new ArrayList<>();
	private IAerospikeClient client;
	private Spikeify sfy;

	@Before
	public void dbSetup() {
		SpikeifyService.globalConfig(namespace, 3000, "localhost");
		client = new AerospikeClient("localhost", 3000);
		sfy = SpikeifyService.sfy();
	}

	@After
	public void dbCleanup() {
		for (String setName : setNames) {
			client.scanAll(null, namespace, setName, new ScanCallback() {
				@Override
				public void scanCallback(Key key, Record record) throws AerospikeException {
					client.delete(null, key);
				}
			});
		}
	}

	@Test
	public void testInfo() {

		setNames.add("set1");
		setNames.add("set2");
		setNames.add("set3");

		int count = 1;
		for (String setName : setNames) {
			Map<Long, EntityOne> map1 = TestUtils.randomEntityOne(count * 10, setName);
			sfy.createAll(map1.values().toArray()).now();
			count++;
		}

		Set<String> infoNamespaces = sfy.info().getNamespaces();
		Assert.assertTrue(infoNamespaces.contains("test"));

		Set<String> infoSetNames = sfy.info().getSets();
		Assert.assertTrue(infoSetNames.contains(setNames.get(0)));
		Assert.assertTrue(infoSetNames.contains(setNames.get(1)));
		Assert.assertTrue(infoSetNames.contains(setNames.get(2)));

	}


}
