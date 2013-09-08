package com.recomdata.pipeline.entrypoint

import org.jboss.weld.environment.se.bindings.Parameters
import org.jboss.weld.environment.se.events.ContainerInitialized
import org.slf4j.Logger

import javax.enterprise.context.ApplicationScoped
import javax.enterprise.event.Event
import javax.enterprise.event.Observes
import javax.enterprise.inject.Any
import javax.enterprise.util.AnnotationLiteral
import javax.inject.Inject
import java.lang.annotation.Annotation

/**
 * A class that serves as a entry point when the application is started using
 * Weld.
 */
@ApplicationScoped
class CDIEntryPoint {

    @Inject private Logger logger;

    @Inject @Any private Event<List<String>> startPipelineEvent

    private final static KNOWN_PIPELINES = [
            'annotation'
    ]

    void main(@Observes ContainerInitialized event, @Parameters List<String> parameters) {

        if (parameters.size() < 1) {
            System.err.println("""
                    The Weld entry point takes at least one argument: the pipeline to run.
                    Known pipelines: $KNOWN_PIPELINES.""".stripIndent())
            System.exit 1
        }

        def pipelineName = parameters[0]

        if (!KNOWN_PIPELINES.contains(pipelineName)) {
            System.err.println "Unknown pipeline: ${pipelineName}"
            System.exit 1
        }

        logger.debug("Will now launch pipeline {}", pipelineName)

        startPipelineEvent.
                select(getPipelineQualifier(pipelineName)).
                fire parameters.drop(1)
    }

    private Annotation getPipelineQualifier(String pipelineName) {
        new PipelineQualifier(value: pipelineName)
    }

    private class PipelineQualifier extends AnnotationLiteral<Pipeline> implements Pipeline {
        String value

        @Override
        String value() {
            value
        }
    }
}
