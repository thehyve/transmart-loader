package com.recomdata.pipeline.util

import groovy.transform.ToString

/**
 * Parameters for a step execution.
 */
@ToString
class StepExecution {

    String stepType

    boolean skip

    Map<String, Object> params

    StepExecution(Map params) {
        if (!params.stepType) {
            throw new IllegalArgumentException('No step type specified')
        }
        this.stepType = params.stepType
        params.remove('stepType')

        if (params.skip) {
            skip = true
        }
        params.remove('skip')

        this.params = params
    }

    Object getAt(String key) {
        params[key]
    }

    void putAt(String key, Object value) {
        params[key] = value
    }
}
