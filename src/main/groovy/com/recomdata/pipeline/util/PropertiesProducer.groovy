package com.recomdata.pipeline.util

import org.slf4j.Logger

import javax.enterprise.context.ApplicationScoped
import javax.enterprise.context.Dependent
import javax.enterprise.inject.Produces
import javax.enterprise.inject.spi.InjectionPoint
import javax.inject.Inject

/**
 * Provides properties files read from the file system.
 */
@ApplicationScoped
class PropertiesProducer {

    @Inject
    private Logger log

    String confLocation = "conf"

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
        def location = fileAnnotation.value()

        if (!cachedProperties.containsKey(location)) {
            Properties properties = new Properties()
            Properties.metaClass.getAsBoolean = { String key ->
                delegate.getProperty(key).
                        toString().toLowerCase().equals("yes")
            }

            InputStream inputStream

            if (location.startsWith('classpath:')) {
                def classpathLocation = (location - 'classpath:') + '.properties'
                log.debug 'Loading properties from classpath resource {}', classpathLocation

                inputStream = Thread.currentThread().contextClassLoader.
                        getResourceAsStream(classpathLocation)
            } else {
                def file = new File(confLocation, location + '.properties')
                log.debug 'Loading properties from filesystem resource {}', file

                inputStream = file.newInputStream()
            }

            cachedProperties[fileAnnotation.value()] =
                inputStream.withStream {
                    properties.load it
                    properties
                }
        } else {
            log.debug 'Returning cached instance for file reference {}', location
        }

        cachedProperties[fileAnnotation.value()]
    }

}
