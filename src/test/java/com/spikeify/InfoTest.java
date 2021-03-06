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

public class InfoTest extends SpikeifyTest {

	private final List<String> setNames = new ArrayList<>();

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

		// check record counts
		Assert.assertEquals(10, sfy.info().getRecordCount(namespace, setNames.get(0)));
		Assert.assertEquals(20, sfy.info().getRecordCount(namespace, setNames.get(1)));
		Assert.assertEquals(30, sfy.info().getRecordCount(namespace, setNames.get(2)));
	}


}
