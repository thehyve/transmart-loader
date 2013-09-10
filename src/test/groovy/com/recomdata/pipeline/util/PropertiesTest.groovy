package com.recomdata.pipeline.util

import com.recomdata.pipeline.it.CDIIntegration
import org.jboss.arquillian.container.test.api.Deployment
import org.jboss.arquillian.junit.Arquillian
import org.jboss.shrinkwrap.api.Archive
import org.junit.Test
import org.junit.runner.RunWith

import javax.enterprise.inject.Any
import javax.enterprise.inject.Instance
import javax.inject.Inject

import static org.hamcrest.CoreMatchers.*
import static org.hamcrest.MatcherAssert.assertThat

@RunWith(Arquillian)
class PropertiesTest {

    @Deployment
    static Archive deploy() {
        CDIIntegration.getJavaArchive().
                addClasses(FileRef, PropertiesProducer)
    }

    @Inject PropertiesProducer propertiesProducer

    @Inject @FileRef('classpath:test')
    Properties classpathLoadedProperties

    @Inject @Any
    Instance<Properties> propertiesLookup;

    @Test
    void testClasspathLoadedProperties() {
        assertThat classpathLoadedProperties.getProperty('foo'), is(equalTo('bar'))
        assertThat classpathLoadedProperties['foo'], is(equalTo('bar'))
    }

    static class SameInstanceHelper {
        @Inject @FileRef('classpath:test') Instance<Properties> instance1
        @Inject @FileRef('classpath:test') Instance<Properties> instance2
    }

    @Inject
    SameInstanceHelper sameInstanceHelper

    @Test
    void testSameInstanceInjected() {
        assertThat sameInstanceHelper.instance1.get(),
                is(sameInstance(sameInstanceHelper.instance2.get()))
    }

    @Test
    void testLoadFromFilesystem() {
        propertiesProducer.confLocation = 'src/test/resources'
        /* we need to do a programmatic lookup because we only change the
         * configuration location in the line before */
        def props = propertiesLookup.select([
                value         : { -> 'test' },
                annotationType: { -> FileRef }
        ] as FileRef).get()
        assertThat props, is(notNullValue())
        assertThat props.getProperty('foo'), is(equalTo('bar'))
    }

    @Test
    void testBooleanProperties() {
        assertThat classpathLoadedProperties.getAsBoolean('booleanVarTrue'), is(true)
        assertThat classpathLoadedProperties.getAsBoolean('booleanVarElse'), is(false)
    }

}
