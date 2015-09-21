package com.spikeify;

import com.aerospike.client.*;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spikeify.entity.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@SuppressWarnings({"unchecked", "UnusedAssignment"})
public class UpdaterTest {

	private final Long userKey1 = new Random().nextLong();
	private final Long userKey2 = new Random().nextLong();
	private final String userKeyString = String.valueOf(new Random().nextLong());
	private final String namespace = "test";
	private final String setName = "newTestSet";
	private Spikeify sfy;
	private IAerospikeClient client;

	@Before
	public void dbSetup() {
		SpikeifyService.globalConfig(namespace, 3000, "localhost");
		client = SpikeifyService.getClient();
		sfy = SpikeifyService.sfy();
	}

	@After
	public void dbCleanup() {
		sfy.truncateNamespace(namespace);
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
		entity.eight = new Date(1420070400);
		entity.eleven = EntityEnum.SECOND;
		entity.unmapped.put("unmap1", 123L);
		entity.unmapped.put("unmap2", "unmapped string");
		entity.unmapped.put("unmap3", 3.14d);
		entity.thirteen = new byte[]{1, 2, 3, 4, 5};

		entity.sub = new EntitySub(333, "something", new Date(1234567L));

		Key key1 = new Key(namespace, setName, userKey1);

		// we did not provide namespace on purpose - let default kick in
		Key saveKey = sfy
				.update(key1, entity)
				.now();

		Key loadKey = new Key(namespace, setName, userKey1);

		Policy policy = new Policy();
		policy.sendKey = true;
		Record record = client.get(policy, loadKey);

		Assert.assertEquals(entity.one, record.getInt("one"));
		Assert.assertEquals(entity.two, record.getString("two"));

		// support for float types is enabled
		if (Value.UseDoubleType) {
			Assert.assertEquals(entity.three, record.getDouble("third"), 0.1); // explicitly set bin name via @BinName annotation
			Assert.assertEquals(entity.four, record.getFloat("four"), 0.1);
		} else {
			Assert.assertEquals(entity.three, record.getDouble("third"), 0.1); // explicitly set bin name via @BinName annotation
			Assert.assertEquals(entity.four, record.getFloat("four"), 0.1);
		}
		Assert.assertEquals(entity.getFive(), record.getShort("five"));
		Assert.assertEquals(entity.getSix(), record.getByte("six"));
		Assert.assertEquals(entity.seven, record.getBoolean("seven"));
		Assert.assertEquals(entity.eight, new Date(record.getLong("eight")));
		Assert.assertEquals(entity.eleven, EntityEnum.valueOf(record.getString("eleven")));
		Assert.assertEquals(entity.unmapped.get("unmap1"), record.getLong("unmap1"));
		Assert.assertEquals(entity.unmapped.get("unmap2"), record.getString("unmap2"));
		Assert.assertEquals(entity.unmapped.get("unmap3"), record.getDouble("unmap3"));
		Assert.assertArrayEquals(entity.thirteen, (byte[]) record.getValue("thirteen"));

		EntitySub subReloaded;
		try {
			subReloaded = new ObjectMapper().readValue(record.getString("sub"), EntitySub.class);
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
		Assert.assertEquals(entity.sub.first, subReloaded.first);
		Assert.assertEquals(entity.sub.second, subReloaded.second);
		assertNull(subReloaded.date); // Json ignored field
	}

	@Test(expected = SpikeifyError.class)
	public void mismatchedKeyType() {

		EntityOne entity = new EntityOne();
		Key keyString = new Key(namespace, setName, userKeyString);

		// we did not provide namespace on purpose - let default kick in
		Key saveKey = sfy
				.update(keyString, entity)
				.now();
	}

	@Test
	public void saveAndLoadProperties() {

		EntityTwo entity = new EntityTwo();

		entity.one = 123;
		entity.two = "a test";
		entity.three = 123.0d;
		entity.four = 123.0f;
		entity.setFive((short) 234);
		entity.setSix((byte) 100);
		entity.seven = true;
		entity.eight = new Date(1420070400);
		entity.eleven = EntityEnum.SECOND;
		entity.sub = new EntitySub(333, "something", new Date(1234567L));
		Key key1 = new Key(namespace, setName, userKeyString);

		// we did not provide namespace on purpose - let default kick in
		Key saveKey = sfy
				.update(key1, entity)
				.now();

		Key loadKey = new Key(namespace, setName, userKeyString);

		EntityTwo reloaded = sfy.get(EntityTwo.class).key(loadKey).now();

		Assert.assertEquals(entity.one, reloaded.one);
		Assert.assertEquals(entity.two, reloaded.two);
		Assert.assertEquals(entity.three, reloaded.three, 0.1);
		Assert.assertEquals(entity.four, reloaded.four, 0.1);
		Assert.assertEquals(entity.getFive(), reloaded.getFive());
		Assert.assertEquals(entity.getSix(), reloaded.getSix());
		Assert.assertEquals(entity.seven, reloaded.seven);
		Assert.assertEquals(entity.eight, reloaded.eight);
		Assert.assertEquals(entity.eleven, reloaded.eleven);
		Assert.assertEquals(entity.sub.first, reloaded.sub.first);
		Assert.assertEquals(entity.sub.second, reloaded.sub.second);
		assertNull(reloaded.sub.date);
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
		entity.eight = new Date(1420070400);
		entity.nine = new ArrayList<>();
		entity.nine.add("one");
		entity.nine.add("two");

		Long saveKey = sfy
				.update(userKey1, entity)
				.namespace(namespace)
				.setName(setName)
				.now();

		// delete entity by hand
		WritePolicy policy = new WritePolicy();
		policy.sendKey = true;
		boolean deleted = client.delete(policy, new Key(namespace, setName, saveKey));
		Assert.assertTrue(deleted); // was indeed deleted

		// change two properties
		entity.one = 100;
		entity.two = "new string";
		entity.nine.add("three");
		entity.thirteen = new byte[]{1, 2, 3, 4, 5};

		sfy.update(userKey1, entity)
				.namespace(namespace)
				.setName(setName)
				.now();

		// reload entity and check that only two properties were updated
		EntityOne reloaded = sfy.get(EntityOne.class)
				.namespace(namespace)
				.setName(setName)
				.key(userKey1)
				.now();

		Assert.assertEquals(reloaded.one, 100);
		Assert.assertEquals(reloaded.two, "new string");
		Assert.assertEquals(reloaded.three, 0, 0.1);
		Assert.assertEquals(reloaded.four, 0, 0.1);
		Assert.assertEquals(reloaded.getFive(), 0);
		Assert.assertEquals(reloaded.getSix(), 0);
		Assert.assertEquals(reloaded.eight, null);
		Assert.assertEquals(reloaded.nine.size(), 3);
		Assert.assertTrue(reloaded.nine.contains("three"));
		Assert.assertArrayEquals(reloaded.thirteen, new byte[]{1, 2, 3, 4, 5});

	}

	@Test
	public void testListUpdate() {

		List aList = new ArrayList();
		aList.add("test1");
		aList.add("test2");
		aList.add(1234);
		aList.add(123.0d);

		Bin bin1 = new Bin("one", aList);
		Bin bin2 = new Bin("two", 1.1f);

		Key saveKey = new Key(namespace, setName, userKey1);

		// save entity manually
		WritePolicy policy = new WritePolicy();
		policy.sendKey = true;
		client.put(policy, saveKey, bin1);

		Record result = client.get(policy, saveKey);

		assertNotNull(result.bins.get("one"));
		Assert.assertEquals(4, ((List) result.bins.get("one")).size());
	}

	@Test
	public void testMapUpdate() {

		Map aMap = new HashMap();
		aMap.put("1", "test1");
		aMap.put("2", "test2");
		aMap.put("3", 1234);
		aMap.put("4", 123.0d);

		Bin bin1 = new Bin("one", aMap);
		Bin bin2 = new Bin("two", 1.1f);
		Bin bin3 = new Bin("three", false);

		Key saveKey = new Key(namespace, setName, userKey1);

		// save entity manually
		WritePolicy policy = new WritePolicy();
		policy.sendKey = true;
		client.put(policy, saveKey, bin1, bin2, bin3);

		Record result = client.get(policy, saveKey);

		assertNotNull(result.bins.get("one"));
		Assert.assertEquals(4, ((Map) result.bins.get("one")).size());
	}

	@Test
	public void entityListMapUpdate() {

		List aList = new ArrayList();
		aList.add("test1");
		aList.add("test2");
		aList.add(1234L);
		aList.add(123.0d);

		Set aSet = new HashSet();
		aSet.add("test1");
		aSet.add("test2");
		aSet.add(1234L);
		aSet.add(123.0d);

		Map aMap = new HashMap();
		aMap.put("1", "testX");
		aMap.put("2", "testY");
		aMap.put("3", 456L);
		aMap.put("4", 456.0d);

		EntityOne entityOne = new EntityOne();
		entityOne.nine = aList;
		entityOne.ten = aMap;
		entityOne.twelve = aSet;

		Key saveKey = new Key(namespace, setName, userKey1);

		// save entity
		WritePolicy policy = new WritePolicy();
		policy.sendKey = true;
		Key savedKey = sfy.update(saveKey, entityOne).now();

		// load entity
		EntityOne loadedEntity = sfy.get(EntityOne.class).key(savedKey).now();

		// check values
		List nine = loadedEntity.nine;
		Map ten = loadedEntity.ten;
		Set twelve = loadedEntity.twelve;
		Assert.assertEquals(4, nine.size());
		Assert.assertEquals(aList, nine);
		Assert.assertEquals(4, ten.size());
		Assert.assertEquals(aMap, ten);
		Assert.assertEquals(4, twelve.size());
		Assert.assertEquals(aSet, twelve);
	}

	@Test
	public void multiPut() {

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
		entity1.userId = userKey1;
		entity1.theSetName = setName;

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
		entity2.userId = userKey2;
		entity2.theSetName = setName;

		// multi-put
		sfy.updateAll(entity1, entity2).now();

		Key key1 = new Key(namespace, setName, userKey1);
		Key key2 = new Key(namespace, setName, userKey2);

		// multi-get
		Map<Key, EntityOne> result = sfy.getAll(EntityOne.class, key1, key2).namespace(namespace).setName(setName).now();

		Assert.assertEquals(2, result.size());
		Assert.assertEquals(entity1, result.get(key1));
		Assert.assertEquals(entity2, result.get(key2));
	}

	@Test
	public void testSkipCacheObjectUpdating() {
		EntityOne entity1 = new EntityOne();
		entity1.userId = 1234L;
		entity1.one = 123;
		entity1.two = "a test";

		sfy.create(entity1).now();

		EntityOne out = sfy.get(EntityOne.class).key(entity1.userId).now();
		Assert.assertEquals(out.generation.intValue(), 1);

		sfy.update(out).now();
		out = sfy.get(EntityOne.class).key(entity1.userId).now();
		Assert.assertEquals(out.generation.intValue(), 1);

		sfy.update(out).forceReplace().now();
		out = sfy.get(EntityOne.class).key(entity1.userId).now();
		Assert.assertEquals(out.generation.intValue(), 2);

		sfy.update(out.userId, out).forceReplace().now();
		out = sfy.get(EntityOne.class).key(entity1.userId).now();
		Assert.assertEquals(out.generation.intValue(), 3);

	}

	@Test
	public void testSkipCacheObjectMultiUpdating() {
		EntityOne entity1 = new EntityOne();
		entity1.userId = 1234L;
		entity1.one = 123;
		entity1.two = "a test";

		EntityOne entity2 = new EntityOne();
		entity2.userId = 12345L;
		entity2.one = 123;
		entity2.two = "a test";

		sfy.createAll(entity1, entity2).now();

		Map<Long, EntityOne> out = sfy.getAll(EntityOne.class, entity1.userId, entity2.userId).now();
		Assert.assertEquals(out.get(entity1.userId).generation.intValue(), 1);
		Assert.assertEquals(out.get(entity2.userId).generation.intValue(), 1);

		sfy.updateAll(out.get(entity1.userId), out.get(entity2.userId)).now();
		out = sfy.getAll(EntityOne.class, entity1.userId, entity2.userId).now();
		Assert.assertEquals(out.get(entity1.userId).generation.intValue(), 1);
		Assert.assertEquals(out.get(entity2.userId).generation.intValue(), 1);

		sfy.updateAll(out.get(entity1.userId), out.get(entity2.userId)).forceReplace().now();
		out = sfy.getAll(EntityOne.class, entity1.userId, entity2.userId).now();
		Assert.assertEquals(out.get(entity1.userId).generation.intValue(), 2);
		Assert.assertEquals(out.get(entity2.userId).generation.intValue(), 2);

		sfy.updateAll(out.get(entity1.userId), out.get(entity2.userId)).forceReplace().now();
		out = sfy.getAll(EntityOne.class, entity1.userId, entity2.userId).now();
		Assert.assertEquals(out.get(entity1.userId).generation.intValue(), 3);
		Assert.assertEquals(out.get(entity2.userId).generation.intValue(), 3);

	}

	@Test
	public void saveNullProperties() {

		EntityNull entity = new EntityNull();
		entity.userId = userKey1;
		entity.value = "test";

		sfy.create(entity)
				.now();

		// reload entity and check that only two properties were updated
		// setName will be implicitly set via Class name
		EntityNull saved = sfy.get(EntityNull.class)
				.key(userKey1)
				.now();

		assertNotNull(saved);
		assertNotNull(saved.userId);
		assertNull(saved.longValue);
	}

	@Test
	public void saveBackToNullProperties() {

		EntityNull entity = new EntityNull();
		entity.userId = userKey1;
		entity.value = "Some value";

		sfy.create(entity)
		   .now();

		// reload entity and check that only two properties were updated
		// setName will be implicitly set via Class name
		EntityNull saved = sfy.get(EntityNull.class)
							  .key(userKey1)
							  .now();

		assertNotNull(saved);
		assertNotNull(saved.userId);
		assertEquals("Some value", saved.value);

		// check if key exist in database
		SpikeifyService.getClient().scanAll(new ScanPolicy(), namespace, EntityNull.class.getSimpleName(), new ScanCallback() {
			@Override
			public void scanCallback(Key key, Record record) throws AerospikeException {
				System.out.println("Key before setting all bins to null: " + key.userKey.toString());
			}
		});

		// set to null
		saved.value = null;

		sfy.update(saved)
		   .now();

		// check if key exist in database
		SpikeifyService.getClient().scanAll(new ScanPolicy(), namespace, EntityNull.class.getSimpleName(), new ScanCallback() {
			@Override
			public void scanCallback(Key key, Record record) throws AerospikeException {
				System.out.println("Key: " + key.userKey.toString());
			}
		});

		EntityNull check = sfy.get(EntityNull.class)
						.key(userKey1)
						.now();
		assertNull(check);

		saved.value = "something";
		sfy.create(saved).now();


		// reload entity and check that only two properties were updated
		// setName will be implicitly set via Class name
		check = sfy.get(EntityNull.class)
							  .key(userKey1)
							  .now();

		assertNotNull(check);
		assertNotNull(check.userId);
		assertNotNull(check.value);
	}

	@Test(expected = SpikeifyError.class)
	public void saveEmptyObject() {

		EntityNull entity = new EntityNull();
		entity.userId = userKey1;

		sfy.create(entity)
						.now();
	}

	@Test
	public void saveLongBackToNullProperties() {

		EntityNull entity = new EntityNull();
		entity.userId = userKey1;
		entity.value = "test";
		entity.longValue = 10L;

		sfy.create(entity)
		   .now();

		// reload entity and check that only two properties were updated
		// setName will be implicitly set via Class name
		EntityNull saved = sfy.get(EntityNull.class)
							  .key(userKey1)
							  .now();

		assertNotNull(saved);
		assertNotNull(saved.userId);
		assertEquals(saved.value, "test");
		assertEquals(10L, saved.longValue.longValue());

		// set to null
		saved.longValue = null;

		sfy.update(saved)
		   .now();

		// reload entity and check that only two properties were updated
		// setName will be implicitly set via Class name
		EntityNull check = sfy.get(EntityNull.class)
							  .key(userKey1)
							  .now();

		assertNotNull(check);
		assertNotNull(check.userId);
		assertNotNull(check.value);
		assertNull(check.longValue);
	}

	@Test
	public void saveLongPrimitiveBackToNullProperties() {

		EntityNull entity = new EntityNull();
		entity.userId = userKey1;
		entity.value = "test";
		entity.longValue = 10L;

		sfy.create(entity)
						.now();

		// reload entity and check that only two properties were updated
		// setName will be implicitly set via Class name
		EntityNull saved = sfy.get(EntityNull.class)
						.key(userKey1)
						.now();

		assertNotNull(saved);
		assertNotNull(saved.userId);
		assertEquals(saved.value, "test");
		assertEquals(10L, saved.longValue.longValue());

		EntityNullV2 checkMapping = sfy.get(EntityNullV2.class)
						.setName(EntityNull.class.getSimpleName())
						.key(userKey1)
						.now();

		assertNotNull(checkMapping);
		assertNotNull(checkMapping.userId);
		assertEquals(checkMapping.value, "test");
		assertEquals(10L, checkMapping.longValue);

		// set to null
		saved.longValue = null;

		sfy.update(saved)
						.now();

		// reload entity and check that only two properties were updated
		// setName will be implicitly set via Class name
		EntityNullV2 check = sfy.get(EntityNullV2.class)
						.setName(EntityNull.class.getSimpleName())
						.key(userKey1)
						.now();

		assertNotNull(check);
		assertNotNull(check.userId);
		assertNotNull(check.value);
		assertEquals("Should be zero ... as null can't be set", 0L, check.longValue);
	}
}
