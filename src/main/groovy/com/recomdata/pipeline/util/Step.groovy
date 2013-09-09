package com.recomdata.pipeline.util

import javax.enterprise.util.AnnotationLiteral
import javax.inject.Qualifier
import java.lang.annotation.ElementType
import java.lang.annotation.Retention
import java.lang.annotation.RetentionPolicy
import java.lang.annotation.Target

/**
 * Qualifies a step event with a step name.
 */

@Qualifier
@Target([ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD])
@Retention(RetentionPolicy.RUNTIME)
public @interface Step {
    /**
     * A unique name for the step
     * @return
     */
    String value()

    static class StepInstance extends AnnotationLiteral<Step> implements Step {
        private String value

        StepInstance(String value) {
            this.value = value
        }

        @Override
        String value() {
            value
        }
    }
}
