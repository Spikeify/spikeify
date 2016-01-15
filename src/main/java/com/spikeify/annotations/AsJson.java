package com.spikeify.annotations;

import com.spikeify.ConversionTarget;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Flags a field should be serialised to JSON
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.TYPE})
public @interface AsJson {

	/**
	 * @return Type of Json conversion. The default is ConversionTarget.DEFAULT, indicating the whole object is converted.
	 */
	ConversionTarget target() default ConversionTarget.DEFAULT;

}

