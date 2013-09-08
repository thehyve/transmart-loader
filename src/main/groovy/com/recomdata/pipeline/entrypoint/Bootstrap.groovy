package com.recomdata.pipeline.entrypoint

import org.apache.log4j.PropertyConfigurator
import org.jboss.weld.environment.se.StartMain

class Bootstrap {

    static main(args) {
        PropertyConfigurator.configure("conf/log4j.properties");

        new StartMain(args).go();
    }
}
