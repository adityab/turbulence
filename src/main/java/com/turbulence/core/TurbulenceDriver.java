package com.turbulence.core;

import java.util.logging.Logger;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.UnknownHostException;

import java.util.HashSet;
import java.util.Set;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

import org.apache.commons.codec.digest.DigestUtils;

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

import org.json.*;

import com.turbulence.util.Config;
import com.turbulence.util.ConfigParseException;
import com.turbulence.util.OntologyMapper;
import com.turbulence.util.OntologySaver;

public class TurbulenceDriver implements Runnable {
    private static final String CLUSTERSPACE_FILE = "clusterspace.db";
    private static final String ONTOLOGY_STORE_DIR = "ontologies";

    private String dataDir = null;

    private ClusterSpace clusterSpace;
    private OWLOntologyManager ontologyManager;
    private OntologyMapper ontologyMapper;
    private Config config;

    private ExecutorService threadPool;

    private Logger logger;

    private BlockingQueue<String> requestQueue;

    public TurbulenceDriver(Config config, BlockingQueue<String> queue) {
        logger = Logger.getLogger(this.getClass().getName());
        this.config = config;

        JSONObject core = config.getSection("core");
        if (core == null)
            throw new ConfigParseException("Missing section 'core'");

        try {
            dataDir = core.getString("data_dir");
            new File(dataDir).mkdir();
        } catch (JSONException e) {
            throw new ConfigParseException("core.data_dir is not a valid entry");
        } catch (SecurityException e) {
            logger.severe("Could not create directory " + dataDir);
            System.exit(1);
        }

        threadPool = Executors.newFixedThreadPool(20);
        setupClusterSpace();
        setupOntologySpace();
        requestQueue = queue;
    }

    private void setupOntologySpace() {
        File ontStore = new File(dataDir, ONTOLOGY_STORE_DIR);
        try {
            ontStore.mkdir();
        } catch (SecurityException e) {
            logger.severe("Could not create " + ontStore.getAbsolutePath());
        }
        ontologyManager = OWLManager.createOWLOntologyManager();
        ontologyMapper = new OntologyMapper(ontStore);
        ontologyManager.addIRIMapper(ontologyMapper);
    }

    private void setupClusterSpace() {
        clusterSpace = new ClusterSpace((new File(dataDir, CLUSTERSPACE_FILE)).getAbsolutePath());
    }

    public void run() {
        try {
            while (true) {
                String url = requestQueue.take();
                registerSchema(url);
                // wait for events
            }
        } catch (InterruptedException e) {
            logger.warning("TurbulenceDriver interrupted");
        }
    }

    public void registerSchema(String url) {
        logger.entering(this.getClass().getName(), "registerSchema", url);
        IRI iri = IRI.create(url);
        try {
            OWLOntology ont = ontologyManager.loadOntology(IRI.create(url));
            if (ontologyMapper.getDocumentIRI(iri) == null)
                threadPool.submit(new OntologySaver(iri, ont, new File(dataDir, ONTOLOGY_STORE_DIR)));
            logger.warning("loaded " + ont);
        } catch (OWLOntologyCreationException e) {
            // TODO handle this doo doo
        }
        // TODO implement rest
    }
}
