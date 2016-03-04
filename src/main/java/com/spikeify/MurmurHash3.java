package com.spikeify;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * An incremental hasher implementing Murmur3 hash function
 * <p>
 * From: https://code.google.com/p/cumulusrdf/source/browse/trunk/src/edu/kit/aifb/cumulus/store/MurmurHash3.java?r=122
 */
public class MurmurHash3 {

	private final long SEED;
	private final State state;
	private int keyLen;
	private byte[] key = new byte[16]; // buffer

	public MurmurHash3(long seed) {
		this.SEED = seed;
		state = new State();
		state.h1 = 0x9368e53c2f6af274L ^ SEED;
		state.h2 = 0x586dcd208f7cd3fdL ^ SEED;

		state.c1 = 0x87c37b91114253d5L;
		state.c2 = 0x4cf5ad432745937fL;
	}

	public MurmurHash3() {
		this(0);
	}

	static class State {
		long h1;
		long h2;

		long k1;
		long k2;

		long c1;
		long c2;
	}

	public MurmurHash3 add(final short data) {
		add((byte) (data & 0x00FF));
		add((byte) ((data & 0xFF00) << 8));

		return this;
	}

	public MurmurHash3 add(final long data) {

		byte[] bytes = new byte[]{
				(byte) (data >> 56),
				(byte) (data >> 48),
				(byte) (data >> 40),
				(byte) (data >> 32),
				(byte) (data >> 24),
				(byte) (data >> 16),
				(byte) (data >> 8),
				(byte) data
		};
		add(bytes);
		return this;
	}

	public synchronized MurmurHash3 add(byte byteData) {

		key[keyLen++ % 16] = byteData;

		if (keyLen % 16 == 0) {
			state.k1 = getblock(0);
			state.k2 = getblock(8);
			bmix(state);
		}

		return this;
	}

	public synchronized MurmurHash3 add(byte[] byteArray) {

		for (byte byteData : byteArray) {
			key[keyLen++ % 16] = byteData;

			if (keyLen % 16 == 0) {
				state.k1 = getblock(0);
				state.k2 = getblock(8);
				bmix(state);
			}
		}

		return this;
	}

	public long[] hash() {
		state.k1 = 0;
		state.k2 = 0;
//		int tail = (keyLen >>> 4) << 4;
		int tail = 0;

		switch (keyLen & 15) {
			case 15:
				state.k2 ^= (long) key[tail + 14] << 48;
			case 14:
				state.k2 ^= (long) key[tail + 13] << 40;
			case 13:
				state.k2 ^= (long) key[tail + 12] << 32;
			case 12:
				state.k2 ^= (long) key[tail + 11] << 24;
			case 11:
				state.k2 ^= (long) key[tail + 10] << 16;
			case 10:
				state.k2 ^= (long) key[tail + 9] << 8;
			case 9:
				state.k2 ^= (long) key[tail + 8];

			case 8:
				state.k1 ^= (long) key[tail + 7] << 56;
			case 7:
				state.k1 ^= (long) key[tail + 6] << 48;
			case 6:
				state.k1 ^= (long) key[tail + 5] << 40;
			case 5:
				state.k1 ^= (long) key[tail + 4] << 32;
			case 4:
				state.k1 ^= (long) key[tail + 3] << 24;
			case 3:
				state.k1 ^= (long) key[tail + 2] << 16;
			case 2:
				state.k1 ^= (long) key[tail + 1] << 8;
			case 1:
				state.k1 ^= (long) key[tail];
				bmix(state);
		}

		state.h2 ^= keyLen;

		state.h1 += state.h2;
		state.h2 += state.h1;

		state.h1 = fmix(state.h1);
		state.h2 = fmix(state.h2);

		state.h1 += state.h2;
		state.h2 += state.h1;

		return new long[]{state.h1, state.h2};
	}

	private static void bmix(State state) {
		state.k1 *= state.c1;
		state.k1 = (state.k1 << 23) | (state.k1 >>> 64 - 23);
		state.k1 *= state.c2;
		state.h1 ^= state.k1;
		state.h1 += state.h2;

		state.h2 = (state.h2 << 41) | (state.h2 >>> 64 - 41);

		state.k2 *= state.c2;
		state.k2 = (state.k2 << 23) | (state.k2 >>> 64 - 23);
		state.k2 *= state.c1;
		state.h2 ^= state.k2;
		state.h2 += state.h1;

		state.h1 = state.h1 * 3 + 0x52dce729;
		state.h2 = state.h2 * 3 + 0x38495ab5;

		state.c1 = state.c1 * 5 + 0x7b7d159c;
		state.c2 = state.c2 * 5 + 0x6bce6396;
	}

	private static long fmix(long k) {
		k ^= k >>> 33;
		k *= 0xff51afd7ed558ccdL;
		k ^= k >>> 33;
		k *= 0xc4ceb9fe1a85ec53L;
		k ^= k >>> 33;

		return k;
	}

	private long getblock(int i) {
		return
				(((long) key[i] & 0x00000000000000FFL))
						| (((long) key[i + 1] & 0x00000000000000FFL) << 8)
						| (((long) key[i + 2] & 0x00000000000000FFL) << 16)
						| (((long) key[i + 3] & 0x00000000000000FFL) << 24)
						| (((long) key[i + 4] & 0x00000000000000FFL) << 32)
						| (((long) key[i + 5] & 0x00000000000000FFL) << 40)
						| (((long) key[i + 6] & 0x00000000000000FFL) << 48)
						| (((long) key[i + 7] & 0x00000000000000FFL) << 56);
	}

}
