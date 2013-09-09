package com.recomdata.pipeline.util

import javax.enterprise.context.ApplicationScoped
import javax.enterprise.context.Dependent
import javax.enterprise.inject.Produces
import javax.enterprise.inject.spi.InjectionPoint

/**
 * Provides properties files read from the file system.
 */
@ApplicationScoped
class PropertiesProducer {

    private final static String CONF_LOCATION = "conf"

    private final Map<String, Properties> cachedProperties = new HashMap()

    /**
     * Injects a properties object by reading the file pointed to by the {@link FileRef}
     * qualifier, which is required.
     *
     * @param injectionPoint
     * @return
     */
    @Produces @Dependent @FileRef('')
    synchronized Properties produceProperties(InjectionPoint injectionPoint) throws IOException {
        FileRef fileAnnotation = injectionPoint.qualifiers.find { it instanceof FileRef }

        if (!cachedProperties.containsKey(fileAnnotation.value())) {
            Properties properties = new Properties()
            Properties.metaClass.getAsBoolean = { String key ->
                delegate.getProperty(key).
                        toString().toLowerCase().equals("yes")
            }

            File file = new File(CONF_LOCATION, fileAnnotation.value() + '.properties')

            cachedProperties[fileAnnotation.value()] =
                file.withInputStream {
                    properties.load it
                    properties
                }
        }

        cachedProperties[fileAnnotation.value()]
    }

}
