package com.spikeify;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.spikeify.entity.EntityExists;
import com.spikeify.entity.EntityNull;
import com.spikeify.entity.EntityOne;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

@SuppressWarnings({"unchecked", "UnusedAssignment"})
public class CreatorTest {

	private final Long userKey1 = new Random().nextLong();
	private final Long userKey2 = new Random().nextLong();
	private final Long userKey3 = new Random().nextLong();
	private final String namespace = "test";
	private final String setName = "newTestSet";
	private Spikeify sfy;

	@Before
	public void dbSetup() {
		SpikeifyService.globalConfig(namespace, 3000, "localhost");
		sfy = SpikeifyService.sfy();
	}

	@After
	public void dbCleanup() {
		sfy.truncateNamespace(namespace);
	}

	@SuppressWarnings("UnusedAssignment")
	@Test
	public void simpleCreate() {

		EntityOne entity = new EntityOne();
		entity.one = 123;
		entity.two = "a test";
		entity.three = 123.0d;
		entity.four = 123.0f;
		entity.setFive((short) 234);
		entity.setSix((byte) 100);
		entity.seven = true;
		entity.eight = new Date(1420070400);
		entity.nine = new ArrayList<>();
		entity.nine.add("one");
		entity.nine.add("two");
		entity.thirteen = new byte[]{1, 2, 3, 4, 5};

		// set Metadata fields
		entity.userId = userKey1;
		entity.theSetName = setName;

		Key saveKey = sfy
				.create(entity)
				.now();

		// reload entity and check that only two properties were updated
		// we did not provide namespace on purpose - let default kick in
		EntityOne reloaded = sfy.get(EntityOne.class)
				.setName(setName)
				.key(userKey1)
				.now();

		Assert.assertEquals(reloaded.one, 123);
		Assert.assertEquals(reloaded.two, "a test");
		Assert.assertEquals(reloaded.three, 123.0d, 0.1);
		Assert.assertEquals(reloaded.four, 123.0f, 0.1);
		Assert.assertEquals(reloaded.getFive(), 234);
		Assert.assertEquals(reloaded.getSix(), 100);
		Assert.assertEquals(reloaded.eight.getTime(), 1420070400);
		Assert.assertEquals(reloaded.userId, userKey1);
		Assert.assertArrayEquals(reloaded.thirteen, new byte[]{1, 2, 3, 4, 5});

	}

	@Test(expected = SpikeifyError.class)
	public void nullCreate() {

		EntityNull entity = new EntityNull();
		entity.userId = userKey1;

		Key key = sfy.create(entity).now();
	}

	@Test
	public void nullCreateUpdate() {

		EntityNull entity = new EntityNull();
		entity.userId = userKey1;
		entity.longValue = 123L;
		entity.value = "krneki";

		Key key = sfy.create(entity).now();
		EntityNull reload = sfy.get(EntityNull.class).key(userKey1).now();
		Assert.assertEquals(reload, entity);

		reload.longValue = null;
		sfy.update(reload).forceReplace().now();
		reload = sfy.get(EntityNull.class).key(userKey1).now();
		Assert.assertNull(reload.longValue);
		// check via client that bin does not exist
		Record record = sfy.getClient().get(null, key);
		Assert.assertEquals(1, record.bins.size());
		Assert.assertEquals("krneki", record.bins.get("value"));
		Assert.assertFalse(record.bins.containsKey("longValue"));

		reload.value = null;
		key = sfy.update(reload).now();
		reload = sfy.get(EntityNull.class).key(userKey1).now();
		Assert.assertNull(reload);
		// check via client that bin does not exist
		boolean exists = sfy.getClient().exists(null, key);
		Assert.assertFalse(exists);
	}

	@Test
	public void nullCreateNative() {

		Key key = new Key(namespace, setName, userKey1);
		sfy.getClient().put(null, key, new Bin[0]);
		boolean exists = sfy.getClient().exists(null, key);
		Assert.assertFalse(exists); // aerospike DB does not save empty records
	}

	@Test
	public void multiCreate() {

		EntityOne entity1 = new EntityOne();
		entity1.one = 123;
		entity1.two = "a test";
		entity1.three = 123.0d;
		entity1.four = 123.0f;
		entity1.setFive((short) 234);
		entity1.setSix((byte) 100);
		entity1.seven = true;
		entity1.eight = new Date(1420070400);
		entity1.nine = new ArrayList<>();
		entity1.nine.add("one");
		entity1.nine.add("two");
		entity1.thirteen = new byte[]{1, 2, 3, 4, 5};

		EntityOne entity2 = new EntityOne();
		entity2.one = 123;
		entity2.two = "a test";
		entity2.three = 123.0d;
		entity2.four = 123.0f;
		entity2.setFive((short) 234);
		entity2.setSix((byte) 100);
		entity2.seven = true;
		entity2.eight = new Date(1420070400);
		entity2.nine = new ArrayList<>();
		entity2.nine.add("one");
		entity2.nine.add("two");
		entity2.thirteen = new byte[]{1, 2, 3, 4, 5};

		Map<Key, Object> saveKeys = sfy
				.createAll(new Long[]{userKey1, userKey2}, new Object[]{entity1, entity2})
				.namespace(namespace)
				.setName(setName)
				.now();

		// reload entity and check that only two properties were updated
		Map<Long, EntityOne> reloaded = sfy.getAll(EntityOne.class, userKey1, userKey2, userKey3)
				.namespace(namespace)
				.setName(setName)
				.now();

		Assert.assertEquals(2, saveKeys.size());
		Assert.assertEquals(2, reloaded.size());

	}

	@Test(expected = com.aerospike.client.AerospikeException.class)
	public void doubleCreate() {

		EntityOne entity = new EntityOne();
		entity.one = 123;
		entity.two = "a test";
		entity.three = 123.0d;
		entity.four = 123.0f;
		entity.setFive((short) 234);
		entity.setSix((byte) 100);
		entity.seven = true;
		entity.eight = new Date(1420070400);
		entity.nine = new ArrayList<>();
		entity.nine.add("one");
		entity.nine.add("two");
		entity.thirteen = new byte[]{1, 2, 3, 4, 5};

		sfy.create(userKey1, entity)
				.namespace(namespace)
				.setName(setName)
				.now();

		sfy.create(userKey1, entity)
				.namespace(namespace)
				.setName(setName)
				.now();
	}

	@Test
	public void createWIthEntityKeyOnly() {

		EntityOne entity = new EntityOne();
		entity.one = 123;
		entity.two = "a test";
		entity.three = 123.0d;
		entity.four = 123.0f;
		entity.setFive((short) 234);
		entity.setSix((byte) 100);
		entity.seven = true;
		entity.eight = new Date(1420070400);
		entity.nine = new ArrayList<>();
		entity.nine.add("one");
		entity.nine.add("two");
		entity.twelve = new HashSet();
		entity.twelve.add("one");
		entity.twelve.add("two");
		entity.thirteen = new byte[]{1, 2, 3, 4, 5};

		Long saveKey = sfy
				.create(userKey1, entity)
				.namespace(namespace)
				.setName(EntityOne.class.getSimpleName()) // use class name as SetName
				.now();

		// reload entity and check that only two properties were updated
		// setName will be implicitly set via Class name
		EntityOne reloaded = sfy.get(EntityOne.class)
				.namespace(namespace)
				.key(userKey1)
				.now();

		Assert.assertEquals(reloaded.one, 123);
		Assert.assertEquals(reloaded.two, "a test");
		Assert.assertEquals(reloaded.three, 123.0d, 0.1);
		Assert.assertEquals(reloaded.four, 123.0f, 0.1);
		Assert.assertEquals(reloaded.getFive(), 234);
		Assert.assertEquals(reloaded.getSix(), 100);
		Assert.assertEquals(reloaded.eight.getTime(), 1420070400);
		Assert.assertEquals(reloaded.nine.size(), 2);
		Assert.assertEquals(reloaded.twelve.size(), 2);
	}

	@Test
	public void checkExists() {

		// create record
		final EntityExists in = TestUtils.randomEntityExists();

		final Key key = sfy.create(in).now();

		// change bin values via set operation
		boolean existsByKey = sfy.exists(key);
		boolean existsById = sfy.exists(EntityExists.class, in.userId);

		Assert.assertTrue(existsByKey);
		Assert.assertTrue(existsById);
	}

	@Test
	public void checkExistsMulti() {

		// create record
		final EntityExists in1 = TestUtils.randomEntityExists();
		final EntityExists in2 = TestUtils.randomEntityExists();

		final Key key1 = sfy.create(in1).now();
		final Key key2 = sfy.create(in2).now();

		// change bin values via set operation
		Map<Key, Boolean> existByKeys = sfy.exist(key1, key2);
		Map<Long, Boolean> existsByIds = sfy.exist(EntityExists.class, in1.userId, in2.userId);

		Assert.assertEquals(2, existByKeys.size());
		Assert.assertTrue(existByKeys.get(key1));
		Assert.assertTrue(existByKeys.get(key2));
		Assert.assertEquals(2, existsByIds.size());
		Assert.assertTrue(existsByIds.get(in1.userId));
		Assert.assertTrue(existsByIds.get(in2.userId));
	}
}
