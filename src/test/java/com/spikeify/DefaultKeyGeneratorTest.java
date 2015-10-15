package com.spikeify;

import org.junit.Test;

import static org.junit.Assert.*;

public class DefaultKeyGeneratorTest {

	@Test
	public void testGenerateString() throws Exception {

		DefaultKeyGenerator generator = new DefaultKeyGenerator();
		String id = generator.generateString(1);
		assertEquals(1, id.length());
		assertTrue(IdGenerator.ELEMENTS.contains(id));

		id = generator.generateString(10);
		String id2 = generator.generateString(10);
		String id3 = generator.generateString(10);
		String id4 = generator.generateString(10);

		// check length
		assertEquals(10, id.length());
		assertEquals(10, id2.length());
		assertEquals(10, id3.length());
		assertEquals(10, id4.length());

		// keys should be different (at least in theory)
		assertNotEquals(id, id2);
		assertNotEquals(id3, id2);
		assertNotEquals(id4, id2);
	}

	@Test
	public void testGenerateLong() throws Exception {

		DefaultKeyGenerator generator = new DefaultKeyGenerator();

		long id = generator.generateLong(10);
		assertEquals(10, Long.toString(id).length());
	}
}