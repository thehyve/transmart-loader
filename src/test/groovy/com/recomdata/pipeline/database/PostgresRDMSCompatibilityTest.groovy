package com.recomdata.pipeline.database

import com.recomdata.pipeline.util.FileRef
import com.recomdata.pipeline.util.PropertiesProducer
import groovy.sql.Sql
import org.jboss.arquillian.container.test.api.Deployment
import org.jboss.arquillian.junit.Arquillian
import org.jboss.shrinkwrap.api.Archive
import org.junit.Test
import org.junit.runner.RunWith

import javax.inject.Inject

import static com.recomdata.pipeline.it.CDIIntegration.createBeansXML
import static com.recomdata.pipeline.it.CDIIntegration.getJavaArchive
import static org.hamcrest.CoreMatchers.*
import static org.hamcrest.MatcherAssert.assertThat

@RunWith(Arquillian)
class PostgresRDMSCompatibilityTest {

    @Deployment
    static Archive deploy() {
        def archive = getJavaArchive().
                addClasses(FileRef,
                        PropertiesProducer,
                        TestCommonPropertiesProducer,
                        RDMS,
                        RDMSCompatibility,
                        RDMSCompatibilityProducer,
                        PostgresRDMSCompatibility,
                        SqlProducer,
                ).
                addAsResource(
                        /* not really needed, we don't use H2 in this test
                         * because its psql compat mode does not support
                         * CREATE TEMP TABLE ... LIKE ...
                         * So we mock Sql instead and check if the command
                         * looks fine */
                        'com/recomdata/pipeline/database/Common-h2-psql.properties',
                        'Common.properties')
        createBeansXML archive, alternatives: [TestCommonPropertiesProducer]
    }

    @Inject Sql sql

    @Inject RDMSCompatibility compatibility

    @Test
    void testCreateTemporaryTable() {
        sql.execute 'CREATE SCHEMA test_schema'
        sql.execute 'CREATE TABLE test_schema.foo(bar int)'

        /* temp table should survive a COMMIT */
        sql.with {
            compatibility.createTemporaryTable 'temp_foo', 'test_schema.foo'
        }

        def number = 43
        sql.execute "INSERT INTO temp_foo VALUES($number)"

        def numberBack = sql.firstRow("SELECT bar FROM temp_foo")[0]

        assertThat number, is(equalTo(numberBack))
    }

}
