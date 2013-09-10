package com.recomdata.pipeline.it

import org.jboss.shrinkwrap.api.ShrinkWrap
import org.jboss.shrinkwrap.api.asset.EmptyAsset
import org.jboss.shrinkwrap.api.spec.JavaArchive

/**
 * Util class to assist with writing integration tests for CDI.
 */
class CDIIntegration {

    static JavaArchive getJavaArchive() {
        ShrinkWrap.create(JavaArchive).
                addPackage('org.jboss.weld.log').
                addAsManifestResource(EmptyAsset.INSTANCE, 'beans.xml')
    }

}
