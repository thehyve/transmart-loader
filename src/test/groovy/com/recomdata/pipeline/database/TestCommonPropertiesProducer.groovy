package com.recomdata.pipeline.database

import com.recomdata.pipeline.util.FileRef
import com.recomdata.pipeline.util.PropertiesProducer

import javax.enterprise.context.Dependent
import javax.enterprise.inject.Alternative
import javax.enterprise.inject.Produces
import javax.enterprise.inject.spi.InjectionPoint
import javax.inject.Inject

class TestCommonPropertiesProducer {

    @Inject PropertiesProducer originalProducer

    @Alternative
    @Produces @Dependent @FileRef('')
    synchronized Properties produceTestCommonProperties(InjectionPoint injectionPoint) {
        FileRef fileAnnotation = injectionPoint.qualifiers.find { it instanceof FileRef }
        def location = fileAnnotation.value()

        if (location == 'Common') {
            location = 'classpath:Common'
        }

        originalProducer.doProduceProperties location
    }
}
