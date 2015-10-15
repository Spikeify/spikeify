package com.spikeify.annotations;

import com.spikeify.DefaultKeyGenerator;
import com.spikeify.UserKeyGenerator;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Flags a field that holds set name of the Record
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
public @interface UserKey {

	boolean generate() default false; // auto generate key on create

	int keyLength() default 10; // default key length if auto-generated

	Class<? extends UserKeyGenerator> generator() default DefaultKeyGenerator.class;
}

