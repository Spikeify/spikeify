package com.spikeify;

import com.aerospike.client.AerospikeException;
import com.spikeify.entity.EntityAutoKey;
import com.spikeify.entity.EntityAutoKey2;
import com.spikeify.entity.EntityAutoKeyBucket;
import com.spikeify.entity.EntityAutoKeyToFail;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class CreateGenerateIdTest {

	private final String namespace = "test";
	private Spikeify sfy;

	@Before
	public void dbSetup() {
		SpikeifyService.globalConfig(namespace, 3000, "localhost");
		sfy = SpikeifyService.sfy();
		sfy.truncateNamespace(namespace);
	}

	@After
	public void dbCleanup() {
		sfy.truncateNamespace(namespace);
	}

	@Test
	public void autogenerateKeyTest() {

		EntityAutoKey entity1 = new EntityAutoKey("A");
		sfy.create(entity1).now();

		EntityAutoKey entity2 = new EntityAutoKey("B");
		sfy.create(entity2).now();

		List<EntityAutoKey> list = sfy.scanAll(EntityAutoKey.class).now();
		assertEquals(2, list.size());

		Set<String> keys = new HashSet<>();
		for (EntityAutoKey item: list) {
			assertEquals(10, item.key.length());
			keys.add(item.key);
		}

		assertEquals(2, keys.size());
	}

	@Test
	public void autogenerateWithRetryTest() {

		// keys are assigned from a pool with duplicated keys
		EntityAutoKeyBucket entity = new EntityAutoKeyBucket("A");
		sfy.create(entity).now();
		assertEquals("A", entity.key);

		EntityAutoKeyBucket entity2 = new EntityAutoKeyBucket("B");
		sfy.create(entity2).now();
		assertEquals("B", entity2.key);

		EntityAutoKeyBucket entity3 = new EntityAutoKeyBucket("C");
		sfy.create(entity3).now();
		assertEquals("C", entity3.key);
	}

	@Test(expected = AerospikeException.class)
	public void failCreatingSecondEntityTest() {

		EntityAutoKeyToFail fail = new EntityAutoKeyToFail("A");
		sfy.create(fail).now();

		try {
			EntityAutoKeyToFail fail2 = new EntityAutoKeyToFail("B");
			sfy.create(fail2).now();
		}
		catch (AerospikeException e) {
			assertEquals("Error Code 5: Key already exists", e.getMessage());
			throw e;
		}
	}

	@Test
	public void multipleCreateTest() {

		EntityAutoKey one = new EntityAutoKey("A");
		EntityAutoKey2 two = new EntityAutoKey2("A");
		EntityAutoKeyBucket three = new EntityAutoKeyBucket("A");
		EntityAutoKeyToFail four = new EntityAutoKeyToFail("A");
		sfy.createAll(one, two, three, four).now();

		EntityAutoKeyToFail compare4 = sfy.get(EntityAutoKeyToFail.class).key(four.key).now();
		EntityAutoKey compare1 = sfy.get(EntityAutoKey.class).key(one.key).now();
		EntityAutoKey2 compare2 = sfy.get(EntityAutoKey2.class).key(two.key).now();
		EntityAutoKeyBucket compare3 = sfy.get(EntityAutoKeyBucket.class).key(three.key).now();

		assertEquals("A", compare1.value);
		assertEquals("A", compare2.value);
		assertEquals("A", compare3.value);
		assertEquals("A", compare4.value);
	}
}

