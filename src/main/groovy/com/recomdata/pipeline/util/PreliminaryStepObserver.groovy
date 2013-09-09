package com.recomdata.pipeline.util

import org.slf4j.Logger

import javax.enterprise.context.ApplicationScoped
import javax.enterprise.event.Event
import javax.enterprise.event.Observes
import javax.inject.Inject

/**
 * Watches for fired step events and decides, logs such event and refires
 * the event in case it's supposed to go forward.
 */
@ApplicationScoped
class PreliminaryStepObserver {

    @Inject Logger logger

    @Inject Event<StepExecution> stepParamsEvent

    void observePreliminary(@Observes @Preliminary StepExecution stepParams) {
        logger.debug('Received preliminary step event. Params: {}', stepParams)

        if (!stepParams.skip) {
            logger.info('Proceeding to step {}...', stepParams.stepType)

            stepParamsEvent.select(new Step.StepInstance(stepParams.stepType)).fire(stepParams)
        } else {
            logger.info('Skipping step {}', stepParams.stepType)
        }
    }


}
