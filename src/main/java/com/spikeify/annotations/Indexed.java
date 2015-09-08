package com.spikeify.annotations;

import com.aerospike.client.query.IndexCollectionType;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
public @interface Indexed {

	/**
	 * specific index name
	 * if not given index name is generated automatically
	 */
	String name() default "";

	/**
	 * Index collection type
	 */
	IndexCollectionType collection() default IndexCollectionType.DEFAULT;
}
