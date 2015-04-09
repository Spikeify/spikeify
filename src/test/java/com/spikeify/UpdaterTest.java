package com.spikeify;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;

public class UpdaterTest {

	private Long userKey = new Random().nextLong();
	private String namespace = "test";
	private String setName = "testSet";

	@Before
	public void dbSetup() {
		SpikeifyService.globalConfig("localhost", 3000);
	}

	@After
	public void dbCleanup() {
		Key deleteKey = new Key(namespace, setName, userKey);
		SpikeifyService.sfy().delete().key(deleteKey).now();
	}

	@Test
	public void saveProperties() {

		EntityOne entity = new EntityOne();

		entity.one = 123;
		entity.two = "a test";
		entity.three = 123.0d;
		entity.four = 123.0f;
		entity.setFive((short) 234);
		entity.setSix((byte) 100);
		entity.seven = true;

		Key saveKey = SpikeifyService.sfy()
				.update(entity)
				.namespace(namespace)
				.set(setName)
				.key(userKey)
				.now();

		Key loadKey = new Key(namespace, setName, userKey);

		AerospikeClient client = new AerospikeClient("localhost", 3000);
		Policy policy = new Policy();
		policy.sendKey = true;
		Record record = client.get(policy, loadKey);

		Assert.assertEquals(entity.one, record.getInt("one"));
		Assert.assertEquals(entity.two, record.getString("two"));
		Assert.assertEquals(entity.three, record.getDouble("three"), 0.1);
		Assert.assertEquals(entity.four, record.getFloat("four"), 0.1);
		Assert.assertEquals(entity.getFive(), record.getShort("five"));
		Assert.assertEquals(entity.getSix(), record.getByte("six"));
		Assert.assertEquals(entity.seven, record.getBoolean("seven"));
	}

	@Test
	public void testDifferentialUpdate() {

		EntityOne entity = new EntityOne();
		entity.one = 123;
		entity.two = "a test";
		entity.three = 123.0d;
		entity.four = 123.0f;
		entity.setFive((short) 234);
		entity.setSix((byte) 100);
		entity.seven = true;

		Key saveKey = SpikeifyService.sfy()
				.update(entity)
				.namespace(namespace)
				.set(setName)
				.key(userKey)
				.now();

		// delete entity by hand
		AerospikeClient client = new AerospikeClient("localhost", 3000);
		WritePolicy policy = new WritePolicy();
		policy.sendKey = true;
		boolean deleted = client.delete(policy, saveKey);
		Assert.assertTrue(deleted); // was indeed deleted

		// change two properties
		entity.one = 100;
		entity.two = "new string";

	  SpikeifyService.sfy()
				.update(entity)
				.namespace(namespace)
				.set(setName)
				.key(userKey)
				.now();

		// reload entity and check that only two properties were updated
		EntityOne reloaded = SpikeifyService.sfy()
				.load(EntityOne.class)
				.namespace(namespace)
				.set(setName)
				.key(userKey)
				.now();

		Assert.assertEquals(reloaded.one, 100);
		Assert.assertEquals(reloaded.two, "new string");
		Assert.assertEquals(reloaded.three, 0, 0.1);
		Assert.assertEquals(reloaded.four, 0, 0.1);
		Assert.assertEquals(reloaded.getFive(), 0);
		Assert.assertEquals(reloaded.getSix(), 0);
	}

}
