package com.spikeify;

import com.aerospike.client.*;
import com.spikeify.entity.EntityOne;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
			EntityOne[] entities = map1.values().toArray(new EntityOne[map1.values().size()]);
			Long[] keys = map1.keySet().toArray(new Long[map1.keySet().size()]);
			sfy.createAll(keys, entities).namespace(namespace).now();
			count++;
		}

		System.out.println(sfy.info().getSets());

	}


}
