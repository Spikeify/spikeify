package com.spikeify;

import com.aerospike.client.*;
import com.aerospike.client.policy.Policy;
import com.spikeify.entity.EntityOne;
import com.spikeify.mock.AerospikeClientMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Random;

public class DeleterTest {

	private Random random = new Random();
	private Long userKeyLong = random.nextLong();
	private String userKeyString = String.valueOf(random.nextLong());
	private String namespace = "test";
	private String setName = "testSet";

	private Spikeify sfy;
	private IAerospikeClient client;

	@Before
	public void dbSetup() {
		SpikeifyService.globalConfig("localhost", 3000, "test");
		client = new AerospikeClientMock();
		sfy = SpikeifyService.mock(client);
	}

	@After
	public void dbCleanup() {
		client.scanAll(null, namespace, setName, new ScanCallback() {
			@Override
			public void scanCallback(Key key, Record record) throws AerospikeException {
				client.delete(null, key);
			}
		});
	}

	@Test
	public void deleteRecordViaKey() {

		EntityOne entity = new EntityOne();
		entity.one = 123;
		entity.two = "a test";
		entity.three = 123.0d;
		entity.four = 123.0f;
		entity.setFive((short) 234);
		entity.setSix((byte) 100);
		entity.seven = true;

		// we did not provide namespace on purpose - let default kick in
		Key saveKey = sfy
				.create(userKeyLong, entity)
				.set(setName)
				.now();

		Key deleteKey = new Key(namespace, setName, userKeyLong);
		sfy.delete(deleteKey).now();

		Policy policy = new Policy();
		policy.sendKey = true;
		boolean exists = client.exists(null, saveKey);

		// assert record does not exist
		Assert.assertFalse(exists);
	}


	@Test
	public void deleteRecord() {

		EntityOne entity = new EntityOne();
		entity.one = 123;
		entity.two = "a test";
		entity.three = 123.0d;
		entity.four = 123.0f;
		entity.setFive((short) 234);
		entity.setSix((byte) 100);
		entity.seven = true;

		Key saveKey = sfy
				.create(userKeyString, entity)
				.namespace(namespace)
				.set(setName)
				.now();

		sfy.delete(userKeyString).namespace(namespace).set(setName).now();

		Policy policy = new Policy();
		policy.sendKey = true;
		boolean exists = client.exists(null, saveKey);

		// assert record does not exist
		Assert.assertFalse(exists);
	}

	@Test
	public void deleteObject() {

		EntityOne entity = TestUtils.randomEntityOne(1, setName).get(0);

		Key saveKey = sfy
				.create(entity)
				.now();

		boolean deleted = sfy.delete(entity).now();
		Assert.assertTrue(deleted);

		Policy policy = new Policy();
		policy.sendKey = true;
		boolean exists = client.exists(null, saveKey);

		// assert record does not exist
		Assert.assertFalse(exists);
	}

	@Test
	public void deleteObjects() {
		List<EntityOne> entities = TestUtils.randomEntityOne(10, setName);
		EntityOne[] antArray = entities.toArray(new EntityOne[entities.size()]);
		Map<Key, Object> res = sfy.createAll(antArray).now();

		for (Key key : res.keySet()) {
			Assert.assertTrue(client.exists(null, key));
		}

		EntityOne[] delEntities = res.values().toArray(new EntityOne[res.size()]);
		Map<Object, Boolean> del = sfy.deleteAll(delEntities).now();
		for (Map.Entry<Object, Boolean> delEntry : del.entrySet()) {
			Assert.assertTrue(delEntry.getValue());
		}
	}

	@Test
	public void deleteKeys() {
		List<EntityOne> entities = TestUtils.randomEntityOne(10, setName);
		EntityOne[] antArray = entities.toArray(new EntityOne[entities.size()]);
		Map<Key, EntityOne> res = sfy.createAll(antArray).now();

		for (Key key : res.keySet()) {
			Assert.assertTrue(client.exists(null, key));
		}

		Map<Key, Boolean> del = sfy.deleteAll(res.keySet().toArray(new Key[res.size()])).now();
		for (Map.Entry<Key, Boolean> delEntry : del.entrySet()) {
			Assert.assertTrue(delEntry.getValue());
			Assert.assertFalse(client.exists(null, delEntry.getKey()));
		}
	}

}
