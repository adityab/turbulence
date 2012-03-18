package com.turbulence;

import java.io.IOException;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;

import org.apache.log4j.BasicConfigurator;

import org.glassfish.grizzly.http.server.HttpServer;
import com.sun.jersey.api.container.grizzly2.GrizzlyServerFactory;
import com.sun.jersey.api.core.PackagesResourceConfig;
import com.sun.jersey.api.core.ResourceConfig;

import com.turbulence.core.TurbulenceDriver;
import com.turbulence.util.Config;

public class Turbulence {
    private HttpServer httpServer;
    private BlockingQueue<String> exitQueue;

    private Turbulence(Config config) {
        exitQueue = new SynchronousQueue<String>();
        // create the clusterspace
        setupDriver(config);
        // start the ontology system
        // start the HTTP server
        setupHTTPServer("http://localhost:5000/");
    }

    /**
     * Call this if the main thread no longer has work to do
     * except in the case when events come in.
     */
    private void loop() {
        try {
            while (true)
                exitQueue.take();
        } catch (InterruptedException e) {
        }
    }

    private void setupDriver(Config config) {
        TurbulenceDriver.initialize(config);
    }

    private void setupHTTPServer(String endpoint) {
        ResourceConfig rc = new PackagesResourceConfig("com.turbulence.rest");
        try {
            GrizzlyServerFactory.createHttpServer(endpoint, rc);
        } catch (IOException e) {
            System.err.println(e.getMessage());
            System.exit(1);
            // TODO: crash here
        }
    }

    public static void main(String[] args) {
        BasicConfigurator.configure();
        // TODO: read the config
        // find config file by parsing options
        // pass config parser instance to HW
        Turbulence hw = new Turbulence(Config.getInstance());
        hw.loop();
    }
}
