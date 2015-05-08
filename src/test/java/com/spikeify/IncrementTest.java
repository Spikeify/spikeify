package com.spikeify;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.spikeify.entity.EntityOne;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class IncrementTest {

	private String namespace = "test";
	private String setName = "testSetQuery";
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
	public void testEntityIncrement() {

		// create record
		final EntityOne ent = TestUtils.randomEntityOne(setName);
		ent.one = 0;
		final Key key = sfy.create(ent).now();

		ExecutorService executor = Executors.newFixedThreadPool(20, Executors.defaultThreadFactory());
		List<Future> futures = new ArrayList<>();

		int increaseCount = 100;
		final int innerIncrease = 100;

		for (int x = 0; x < increaseCount; x++) {

			Future<?> future = executor.submit(new Runnable() {
				@Override
				public void run() {
					for (int i = 0; i < innerIncrease; i++) {
						sfy.command(EntityOne.class).setName(setName).key(ent.userId).add("one", 1).now();  // decrement field 'one' by -1
					}
				}
			});

			futures.add(future);
		}

		for (Future future : futures) {
			try {
				future.get();
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
		}

		EntityOne out = sfy.get(EntityOne.class).key(key).now();

		Assert.assertEquals(increaseCount * innerIncrease, out.one);
		executor.shutdown();
	}

}