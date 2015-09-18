package com.spikeify;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;

import java.lang.reflect.Field;

public abstract class BigDatatypeWrapper {

	public abstract void init(AerospikeClient client, Key key, String binName, Field field);

}
