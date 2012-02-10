package com.turbulence;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.UnknownHostException;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;

import javax.xml.parsers.*;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.util.FileUtils;
import org.semanticweb.owlapi.io.*;
import org.semanticweb.owlapi.model.*;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.semanticweb.owlapi.apibinding.*;

import org.semanticweb.owlapi.reasoner.NodeSet;
import org.semanticweb.owlapi.reasoner.Node;
import org.semanticweb.owlapi.util.SimpleIRIMapper;
import org.semanticweb.owlapi.util.*;
import com.clarkparsia.pellet.owlapiv3.*;

import org.glassfish.grizzly.http.server.HttpServer;
import com.sun.jersey.api.container.grizzly2.GrizzlyServerFactory;
import com.sun.jersey.api.core.PackagesResourceConfig;
import com.sun.jersey.api.core.ResourceConfig;

import com.turbulence.util.OntologyMapper;

public class HelloWorld {
    private ClusterSpace clusterSpace;
    private HttpServer httpServer;
    private BlockingQueue<String> exitQueue;

    private static enum Rels implements RelationshipType {
        IS_A,
        ASSOCIATION
    }

    private HelloWorld() {
        exitQueue = new SynchronousQueue<String>();
        // create the clusterspace
        setupClusterSpace("/tmp/clusterspace.db");
        // start the ontology system
        // start the HTTP server
        setupHTTPServer("http://localhost:5000/");
    }

    /**
     * Call this if the main thread no longer has work to do
     * except in the case when events come in.
     */
    private void loop() {
        // TODO fix this to setup message passing
        // and then wait properly
        // wait until someone tells us otherwise
        try {
            exitQueue.take();
        } catch (InterruptedException e) {
        }
    }

    private void setupClusterSpace(String dbPath) {
        clusterSpace = new ClusterSpace(dbPath);
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
        // TODO: read the config
        // find config file by parsing options
        // pass config parser instance to HW
        HelloWorld hw = new HelloWorld();
        hw.loop();
    }
}
