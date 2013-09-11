package com.recomdata.pipeline.it

import groovy.xml.MarkupBuilder
import org.jboss.shrinkwrap.api.ShrinkWrap
import org.jboss.shrinkwrap.api.asset.EmptyAsset
import org.jboss.shrinkwrap.api.asset.StringAsset
import org.jboss.shrinkwrap.api.spec.JavaArchive

import java.lang.annotation.Annotation

/**
 * Util class to assist with writing integration tests for CDI.
 */
class CDIIntegration {

    static JavaArchive getJavaArchive() {
        ShrinkWrap.create(JavaArchive).
                addPackage('org.jboss.weld.log').
                addAsManifestResource(EmptyAsset.INSTANCE, 'beans.xml')
    }

    /**
     * Adds a non-empty beans.xml to the JavaArchive.
     *
     * @param elements A map with the following optional keys:  decorators,
     *                 alternatives and interceptors. They should be lists of
     *                 Class objects.
     * @param archive  The archive to which the beans.xml file should be added.
     * @return The passed in archive.
     */
    static JavaArchive createBeansXML(Map elements, JavaArchive archive) {
        def writer = new StringWriter()
        def xml = new MarkupBuilder(writer)

        xml.beans(xmlns: 'http://java.sun.com/xml/ns/javaee') {
            decorators {
                for (Class decorator in elements.decorators) {
                    delegate.'class' decorator.canonicalName /* class is a keyword */
                }
            }

            alternatives {
                for (Class alternative in elements.alternatives) {
                    if (Annotation.isAssignableFrom(Class)) {
                        stereotype alternative.canonicalName
                    } else {
                        delegate.'class' alternative.canonicalName
                    }
                }
            }

            interceptors {
                for (Class interceptor in elements.interceptors) {
                    delegate.'class' interceptor.canonicalName
                }
            }
        }

        archive.addAsManifestResource(new StringAsset(writer.toString()), 'beans.xml')
    }

}
