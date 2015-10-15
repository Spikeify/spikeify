package com.spikeify;

import com.aerospike.client.AerospikeException;
import com.spikeify.entity.EntityAutoKey;
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
	}

	@After
	public void dbCleanup() {
		sfy.truncateNamespace(namespace);
	}

	@Test
	public void autogenerateKeyTest() {

		EntityAutoKey entity1 = new EntityAutoKey();
		entity1.value = "one";
		sfy.create(entity1).now();

		EntityAutoKey entity2 = new EntityAutoKey();
		entity2.value = "two";
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

	}

	@Test(expected = AerospikeException.class)
	public void failCreatingSecondEntityTest() {

		EntityAutoKeyToFail fail = new EntityAutoKeyToFail();
		fail.value = "A";
		sfy.create(fail).now();

		try {
			EntityAutoKeyToFail fail2 = new EntityAutoKeyToFail();
			fail2.value = "B";
			sfy.create(fail2).now();
		}
		catch (AerospikeException e) {
			assertEquals("Error Code 5: Key already exists", e.getMessage());
			throw e;
		}
	}
}

