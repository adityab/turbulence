package com.turbulence.core;

import java.util.logging.Logger;

import java.io.File;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.semanticweb.owlapi.model.OWLOntologyManager;

import org.json.*;

import com.turbulence.util.Config;
import com.turbulence.util.ConfigParseException;
import com.turbulence.util.OntologyMapper;

public class TurbulenceDriver {
    private static final String CLUSTERSPACE_FILE = "clusterspace.db";
    private static final String ONTOLOGY_STORE_DIR = "ontologies";

    private static String dataDir = null;

    private static ClusterSpace clusterSpace;

    private OWLOntologyManager ontologyManager;
    private OntologyMapper ontologyMapper;
    private static Config config;

    private static ExecutorService threadPool;

    private static Logger logger;

    public static void initialize(Config config) {
        logger = Logger.getLogger("com.turbulence.core.TurbulenceDriver");
        config = config;

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
    }

    /*private void setupOntologySpace() {
        File ontStore = new File(dataDir, ONTOLOGY_STORE_DIR);
        try {
            ontStore.mkdir();
        } catch (SecurityException e) {
            logger.severe("Could not create " + ontStore.getAbsolutePath());
        }
        ontologyManager = OWLManager.createOWLOntologyManager();
        ontologyMapper = new OntologyMapper(ontStore);
        ontologyManager.addIRIMapper(ontologyMapper);
    }*/

    public static ClusterSpace getClusterSpace() {
        synchronized (TurbulenceDriver.class) {
            if (clusterSpace == null)
                clusterSpace = new ClusterSpace((new File(dataDir, CLUSTERSPACE_FILE)).getAbsolutePath());
        }
        return clusterSpace;
    }

    public static <T> Future<T> submit(Callable<T> task) {
        return threadPool.submit(task);
    }

    public static Future<?> submit(Runnable task) {
        return threadPool.submit(task);
    }

    public static File getOntologyStoreDirectory() {
        return new File(dataDir, ONTOLOGY_STORE_DIR);
    }
}
