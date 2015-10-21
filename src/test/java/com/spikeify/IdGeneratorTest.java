package com.spikeify;

import com.spikeify.entity.EntityAutoKey;
import com.spikeify.entity.EntityAutoKey2;
import com.spikeify.entity.EntityAutoKeyToFail;
import com.spikeify.entity.EntityOne;
import org.junit.Test;

import static org.junit.Assert.*;

public class IdGeneratorTest {

	@Test
	public void shouldGenerateIdTest() {

		EntityOne one = new EntityOne();
		assertFalse(IdGenerator.shouldGenerateId(one));

		EntityAutoKey auto = new EntityAutoKey("A");
		assertTrue(IdGenerator.shouldGenerateId(auto));
	}

	@Test
	public void generateId() {

		// 0
		EntityAutoKey auto = new EntityAutoKey("A");
		IdGenerator.generateId(auto);

		assertNotNull(auto.getKey());
		assertEquals(10, auto.getKey().length());

		// 1
		EntityAutoKey2 auto2 = new EntityAutoKey2("A");
		IdGenerator.generateId(auto2);

		assertNotNull(auto2.getKey());
		assertEquals(2, auto2.getKey().toString().length());

		// 2
		EntityAutoKeyToFail auto3 = new EntityAutoKeyToFail("A");
		IdGenerator.generateId(auto3);

		assertNotNull(auto3.getKey());
		assertEquals(1, auto3.getKey());
	}
}