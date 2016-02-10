package com.spikeify;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.policy.Policy;
import com.spikeify.entity.EntityOne;
import com.spikeify.entity.EntityTwo;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@SuppressWarnings("SuspiciousToArrayCall")
public class DeleterTest extends SpikeifyTest {

	private final Random random = new Random();
	private final Long userKeyLong = random.nextLong();
	private final String userKeyString = String.valueOf(random.nextLong());
	private final String setName = "testSet";

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
		Long saveKey = sfy
				.create(userKeyLong, entity)
				.setName(setName)
				.now();

		Key deleteKey = new Key(namespace, setName, userKeyLong);
		sfy.delete(deleteKey).now();

		Policy policy = new Policy();
		policy.sendKey = true;
		boolean exists = client.exists(null, new Key(namespace, setName, saveKey));

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

		Long saveKey = sfy
				.create(userKeyLong, entity)
				.namespace(namespace)
				.setName(setName)
				.now();

		sfy.delete(userKeyLong).namespace(namespace).setName(setName).now();

		Policy policy = new Policy();
		policy.sendKey = true;
		boolean exists = client.exists(null, new Key(namespace, setName, saveKey));

		// assert record does not exist
		Assert.assertFalse(exists);
	}

	@Test
	public void deleteObject() {

		EntityOne entity = TestUtils.randomEntityOne(setName);

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
		Map<Long,EntityOne> entities = TestUtils.randomEntityOne(10, setName);
		EntityOne[] antArray = entities.values().toArray(new EntityOne[entities.size()]);
		Map<Key, Object> res = sfy.createAll((Object[])antArray).now();

		for (Key key : res.keySet()) {
			Assert.assertTrue(client.exists(null, key));
		}

		EntityOne[] delEntities = res.values().toArray(new EntityOne[res.size()]);
		Map<Object, Boolean> del = sfy.deleteAll((Object[])delEntities).now();
		for (Map.Entry<Object, Boolean> delEntry : del.entrySet()) {
			Assert.assertTrue(delEntry.getValue());
		}
	}

	@Test
	public void deleteKeys() {
		Map<Long, EntityOne> entities = TestUtils.randomEntityOne(10, setName);
		EntityOne[] antArray = entities.values().toArray(new EntityOne[entities.size()]);
		Map<Key, Object> res = sfy.createAll((Object[])antArray).now();

		for (Key key : res.keySet()) {
			Assert.assertTrue(client.exists(null, key));
		}

		Map<Key, Boolean> del = sfy.deleteAll(res.keySet().toArray(new Key[res.size()])).now();
		for (Map.Entry<Key, Boolean> delEntry : del.entrySet()) {
			Assert.assertTrue(delEntry.getValue());
			Assert.assertFalse(client.exists(null, delEntry.getKey()));
		}
	}

	@Test
	public void deleteAllKeys() {
		Map<Long, EntityOne> entities = TestUtils.randomEntityOne(10, setName);
		EntityOne[] antArray = entities.values().toArray(new EntityOne[entities.size()]);
		Map<Key, Object> res = sfy.createAll((Object[])antArray).now();

		for (Key key : res.keySet()) {
			Assert.assertTrue(client.exists(null, key));
		}

		Map<Key, Boolean> del = sfy.deleteAll(EntityOne.class, res.keySet().toArray(new Key[res.size()])).now();
		for (Map.Entry<Key, Boolean> delEntry : del.entrySet()) {
			Assert.assertTrue(delEntry.getValue());
			Assert.assertFalse(client.exists(null, delEntry.getKey()));
		}

	}

	@Test
	public void deleteAllKeysAsync() throws ExecutionException, InterruptedException {
		Map<Long, EntityOne> entities = TestUtils.randomEntityOne(10, setName);
		EntityOne[] antArray = entities.values().toArray(new EntityOne[entities.size()]);
		Map<Key, Object> res = sfy.createAll((Object[])antArray).now();

		for (Key key : res.keySet()) {
			Assert.assertTrue(client.exists(null, key));
		}

		Future<Map<Key, Boolean>> delFuture = sfy.deleteAll(EntityOne.class, res.keySet().toArray(new Key[res.size()])).async();

		Map<Key, Boolean> del = delFuture.get();

		for (Map.Entry<Key, Boolean> delEntry : del.entrySet()) {
			Assert.assertTrue(delEntry.getValue());
			Assert.assertFalse(client.exists(null, delEntry.getKey()));
		}

	}

	@Test
	public void deleteAllLongs() {
		Map<Long, EntityOne> entities = TestUtils.randomEntityOne(10, EntityOne.class.getSimpleName());
		EntityOne[] antArray = entities.values().toArray(new EntityOne[entities.size()]);
		Map<Key, Object> res = sfy.createAll(antArray).now();

		for (Key key : res.keySet()) {
			Assert.assertTrue(client.exists(null, key));
		}

		Assert.assertTrue(null != sfy.get(EntityOne.class).key(entities.keySet().iterator().next()));

		Map<Key, Boolean> del = sfy.deleteAll(EntityOne.class, entities.keySet().toArray(new Long[entities.keySet().size()])).now();
		for (Map.Entry<Key, Boolean> delEntry : del.entrySet()) {
			Assert.assertTrue(delEntry.getValue());
			Assert.assertFalse(client.exists(null, delEntry.getKey()));
		}

	}

	@Test
	public void deleteAllStrings() {
		Map<String, EntityTwo> entities = TestUtils.randomEntityTwo(10, EntityTwo.class.getSimpleName());
		EntityTwo[] antArray = entities.values().toArray(new EntityTwo[entities.size()]);
		Map<Key, Object> res = sfy.createAll(antArray).now();
		for (Key key : res.keySet()) {
			Assert.assertTrue(client.exists(null, key));
		}

		Assert.assertTrue(null != sfy.get(EntityTwo.class).key(entities.keySet().iterator().next()));

		Map<Key, Boolean> del = sfy.deleteAll(EntityTwo.class, entities.keySet().toArray(new String[entities.keySet().size()])).now();
		for (Map.Entry<Key, Boolean> delEntry : del.entrySet()) {
			Assert.assertTrue(delEntry.getValue());
			Assert.assertFalse(client.exists(null, delEntry.getKey()));
		}

	}

}
