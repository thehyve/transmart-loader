package com.recomdata.pipeline.util

import javax.enterprise.util.Nonbinding
import javax.inject.Qualifier
import java.lang.annotation.ElementType
import java.lang.annotation.Retention
import java.lang.annotation.RetentionPolicy
import java.lang.annotation.Target

/**
 * Qualifies an injection point with a file reference.
 */
@Qualifier
@Target([ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD])
@Retention(RetentionPolicy.RUNTIME)
public @interface FileRef {
    /**
     * A reference to the associated file. Whether this is an URL, relative
     * or absolute path etc. is not specified.
     * @return
     */
    @Nonbinding
    String value()
}
