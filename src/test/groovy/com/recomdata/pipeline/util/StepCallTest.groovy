package com.recomdata.pipeline.util

import com.recomdata.pipeline.it.CDIIntegration
import org.jboss.arquillian.container.test.api.Deployment
import org.jboss.arquillian.junit.Arquillian
import org.jboss.shrinkwrap.api.Archive
import org.junit.Test
import org.junit.runner.RunWith

import javax.enterprise.event.Event
import javax.enterprise.event.Observes
import javax.inject.Inject

import static org.hamcrest.CoreMatchers.is
import static org.hamcrest.CoreMatchers.nullValue
import static org.hamcrest.MatcherAssert.assertThat

@RunWith(Arquillian)
class StepCallTest {

    @Inject @Preliminary
    Event<StepExecution> event

    @Deployment
    static Archive deploy() {
        CDIIntegration.getJavaArchive().
                addClasses(StepExecution, Step, Preliminary, PreliminaryStepObserver)
    }

    static class TestStepObserver {
        void observer(@Observes @Step('test') StepExecution stepExec) {
            stepExec['mark'] = true
        }
    }

    @Test
    void basicTest() {
        /* test a basic success step execution */
        def execution = new StepExecution(stepType: 'test')
        event.fire execution
        assertThat execution['mark'], is(true)
    }

    @Test
    void basicSkipTest() {
        def execution = new StepExecution(stepType: 'test', skip: true)
        event.fire execution
        assertThat execution['mark'], is(nullValue())
    }
}
