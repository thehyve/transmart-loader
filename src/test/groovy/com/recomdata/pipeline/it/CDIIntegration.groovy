package com.recomdata.pipeline.it

import org.jboss.shrinkwrap.api.ShrinkWrap
import org.jboss.shrinkwrap.api.asset.EmptyAsset
import org.jboss.shrinkwrap.api.spec.JavaArchive

/**
 * Mixin to assist on writing integration tests for CDI.
 */
class CDIIntegration {

    static JavaArchive getJavaArchive() {
        ShrinkWrap.create(JavaArchive).
                addPackage('org.jboss.weld.log').
                addAsManifestResource(EmptyAsset.INSTANCE, 'beans.xml')
    }

}
