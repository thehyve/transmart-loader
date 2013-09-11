package com.recomdata.pipeline.database

import javax.enterprise.util.Nonbinding
import javax.inject.Qualifier
import java.lang.annotation.ElementType
import java.lang.annotation.Retention
import java.lang.annotation.RetentionPolicy
import java.lang.annotation.Target

/**
 * Qualifies an injection point a RDMS reference.
 */
@Qualifier
@Target([ElementType.TYPE, ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD])
@Retention(RetentionPolicy.RUNTIME)
public @interface RDMS {
    /**
     * The name of the RDMS system.
     * @return
     */
    String value()
}
