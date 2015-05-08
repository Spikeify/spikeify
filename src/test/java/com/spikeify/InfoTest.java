package com.spikeify;

import com.spikeify.entity.EntityOne;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class InfoTest {

	private final String namespace = "test";
	private final List<String> setNames = new ArrayList<>();
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
	public void testInfo() {

		setNames.add("set1");
		setNames.add("set2");
		setNames.add("set3");

		int count = 1;
		for (String setName : setNames) {
			Map<Long, EntityOne> map1 = TestUtils.randomEntityOne(count * 10, setName);
			sfy.createAll(map1.values().toArray()).now();
			count++;
		}

		Set<String> infoNamespaces = sfy.info().getNamespaces();
		Assert.assertTrue(infoNamespaces.contains("test"));

		Map<String, String> infoSetNames = sfy.info().getSets();
		Assert.assertTrue(infoSetNames.keySet().contains(setNames.get(0)));
		Assert.assertTrue(infoSetNames.keySet().contains(setNames.get(1)));
		Assert.assertTrue(infoSetNames.keySet().contains(setNames.get(2)));
	}


}
