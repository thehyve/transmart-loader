package com.recomdata.pipeline.util

import javax.inject.Qualifier
import java.lang.annotation.ElementType
import java.lang.annotation.Retention
import java.lang.annotation.RetentionPolicy
import java.lang.annotation.Target

/**
 * A qualifier for preliminary (to be filtered by
 * {@link PreliminaryStepObserver}) step events.
 */

@Qualifier
@Target([ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD])
@Retention(RetentionPolicy.RUNTIME)
public @interface Preliminary {}
