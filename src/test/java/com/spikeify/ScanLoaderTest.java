package com.spikeify;

import com.aerospike.client.Value;
import com.spikeify.commands.AcceptFilter;
import com.spikeify.entity.EntityOne;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ScanLoaderTest {

	private final String namespace = "test";
	private Spikeify sfy;

	@Before
	public void dbSetup() {

		SpikeifyService.globalConfig(namespace, 3000, "localhost");
		sfy = SpikeifyService.sfy();
		sfy.truncateNamespace(namespace);
	}

	@Test
	public void scanLoaderTest() {

		Set<Long> expected = new HashSet<>();
		Set<Long> checkExpected = new HashSet<>();

		for (int i = 0; i < 100; i++) {
			EntityOne entity = new EntityOne();
			entity.userId = (long) i;
			entity.one = i;

			sfy.create(entity).now();

			expected.add(entity.userId);
		}

		List<EntityOne> all = sfy.scanAll(EntityOne.class).now();

		assertEquals(100, all.size());

		// do we have them all?
		for (EntityOne one : all) {
			assertTrue(expected.contains(one.userId));
			checkExpected.add(one.userId);

			assertEquals(one.userId.longValue(), (long) one.one);
		}

		assertEquals(expected.size(), checkExpected.size());

		// scan with max records set
		checkExpected.clear();

		List<EntityOne> notAll = sfy.scanAll(EntityOne.class).maxRecords(20).now();
		assertEquals(20, notAll.size());

		// do we have them all?
		for (EntityOne one : notAll) {
			assertTrue(expected.contains(one.userId));
			checkExpected.add(one.userId);

			assertEquals(one.userId.longValue(), (long) one.one);
		}

		assertEquals(20, checkExpected.size());
	}

	@Test
	public void scanLoaderWithFilterTest() {

		for (int i = 0; i < 100; i++) {
			EntityOne entity = new EntityOne();
			entity.userId = (long) i;
			entity.one = i;

			sfy.create(entity).now();
		}

		List<EntityOne> all = sfy.scanAll(EntityOne.class).filter(new AcceptFilter<EntityOne>() {
			@Override
			public boolean accept(EntityOne item) {

				return (item.one < 10);
			}
		}).now();

		assertEquals(10, all.size());
		for (EntityOne one : all) {
			assertTrue(one.one < 10);
		}
	}

	@Test
	public void scanLoaderKeysOnly() {

		Set<Long> checkExpected = new HashSet<>();
		for (int i = 0; i < 100; i++) {
			EntityOne entity = new EntityOne();
			entity.userId = (long) i; // key
			entity.one = i;

			sfy.create(entity).now();

			checkExpected.add(entity.userId);
		}

		List<Value> all = sfy.scanAll(EntityOne.class).keys();

		assertEquals(100, all.size());
		assertEquals(100, checkExpected.size());

		for (Value key : all) {
			assertTrue(checkExpected.contains(key.toLong()));
		}
	}
}