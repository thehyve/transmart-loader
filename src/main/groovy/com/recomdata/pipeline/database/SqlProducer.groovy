package com.recomdata.pipeline.database

import com.recomdata.pipeline.util.FileRef
import groovy.sql.Sql

import javax.annotation.PreDestroy
import javax.enterprise.context.ApplicationScoped
import javax.enterprise.inject.Disposes
import javax.enterprise.inject.Produces
import javax.inject.Inject
import java.sql.DriverManager

@ApplicationScoped
class SqlProducer {

    @Inject @FileRef('Common') Properties common;

    private Sql sql

    /* Sql is not proxyable, so we have to returning a dependent bean is the best
     * option here. */
    @Produces synchronized Sql createTmCzSql() {
        if (sql == null) {
            String driver = common.driver_class
            String url = common.url
            String user = common.tm_cz_username
            String password = common.tm_cz_password

            loadDriver driver
            def conn = DriverManager.getConnection url, user, password
            conn.autoCommit = false

            this.sql = new Sql(conn)
        }

        sql
    }

    @PreDestroy
    void destroy() {
        if (sql) {
            sql.close()
        }
    }

    static loadDriver(String driverClassName) {
        try {
            Thread.currentThread().contextClassLoader.loadClass driverClassName
        } catch (ClassNotFoundException e) {
            try {
                this.classLoader.loadClass driverClassName
            }
            catch (ClassNotFoundException e2) {
                    throw e2;
            }
        }
    }

}
