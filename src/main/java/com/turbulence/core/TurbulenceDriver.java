package com.turbulence.core;

import java.util.Arrays;
import java.util.HashMap;

import java.util.logging.Logger;

import java.io.File;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import java.util.Map;

import org.json.*;

import com.turbulence.util.Config;
import com.turbulence.util.ConfigParseException;

import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;

import me.prettyprint.cassandra.serializers.StringSerializer;

import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.SuperCfTemplate;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ThriftSuperCfTemplate;

import me.prettyprint.cassandra.service.ThriftKsDef;

import me.prettyprint.hector.api.Cluster;

import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ColumnType;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;

import me.prettyprint.hector.api.factory.HFactory;

import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;

public class TurbulenceDriver {
    private static final String CLUSTERSPACE_FILE = "clusterspace.db";
    private static final String ONTOLOGY_STORE_DIR = "ontologies";

    private static String dataDir = null;

    private static ClusterSpace clusterSpace;

    private static Config config;

    private static ExecutorService threadPool;

    private static Logger logger;

    private static Cluster cassandra;
    private static Keyspace keyspace;
    private static ColumnFamilyTemplate<String, String> conceptsTemplate;
    private static ColumnFamilyTemplate<String, String> instanceDataTemplate;
    private static SuperCfTemplate<String, String, String> spoDataTemplate;
    private static SuperCfTemplate<String, String, String> opsDataTemplate;

    public static void initialize(Config config) {
        logger = Logger.getLogger("com.turbulence.core.TurbulenceDriver");
        TurbulenceDriver.config = config;

        JSONObject core = TurbulenceDriver.config.getSection("core");
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

        cassandra = HFactory.getOrCreateCluster("turbulence", "localhost:9160");
        ColumnFamilyDefinition conceptsCf = HFactory.createColumnFamilyDefinition("Turbulence",
                "Concepts",
                ComparatorType.UTF8TYPE);
        conceptsCf.setKeyValidationClass("UTF8Type");

        Map<String, String> compressionOptions = new HashMap<String, String>();
        compressionOptions.put("sstable_compression", "SnappyCompressor");
        compressionOptions.put("chunk_length_kb", "64");

        ColumnFamilyDefinition instanceDataCf = HFactory.createColumnFamilyDefinition("Turbulence", "InstanceData", ComparatorType.UTF8TYPE);
        instanceDataCf.setKeyValidationClass("UTF8Type");
        instanceDataCf.setCompressionOptions(compressionOptions);

        ColumnFamilyDefinition spoDataCf = HFactory.createColumnFamilyDefinition("Turbulence", "SPOData", ComparatorType.UTF8TYPE);
        spoDataCf.setColumnType(ColumnType.SUPER);
        spoDataCf.setKeyValidationClass("UTF8Type");
        spoDataCf.setCompressionOptions(compressionOptions);

        ColumnFamilyDefinition opsDataCf = HFactory.createColumnFamilyDefinition("Turbulence", "OPSData", ComparatorType.UTF8TYPE);
        opsDataCf.setColumnType(ColumnType.SUPER);
        opsDataCf.setKeyValidationClass("UTF8Type");
        opsDataCf.setCompressionOptions(compressionOptions);

        KeyspaceDefinition kspd = cassandra.describeKeyspace("Turbulence");
        if (kspd == null) {
            KeyspaceDefinition newKeyspace = HFactory.createKeyspaceDefinition("Turbulence", ThriftKsDef.DEF_STRATEGY_CLASS, 1 /*TODO replication factor*/, Arrays.asList(conceptsCf, instanceDataCf, spoDataCf, opsDataCf));
            cassandra.addKeyspace(newKeyspace, true);
        }
        // FIXME: in a cluster we don't want this
        ConfigurableConsistencyLevel lvl = new ConfigurableConsistencyLevel();
        lvl.setDefaultReadConsistencyLevel(HConsistencyLevel.ONE);
        lvl.setDefaultWriteConsistencyLevel(HConsistencyLevel.ONE);
        keyspace = HFactory.createKeyspace("Turbulence", cassandra, lvl);

        conceptsTemplate = new ThriftColumnFamilyTemplate<String, String>(keyspace, "Concepts", StringSerializer.get(), StringSerializer.get());
        instanceDataTemplate = new ThriftColumnFamilyTemplate<String, String>(keyspace, "InstanceData", StringSerializer.get(), StringSerializer.get());
        spoDataTemplate = new ThriftSuperCfTemplate<String, String, String>(keyspace, "SPOData", StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
        opsDataTemplate = new ThriftSuperCfTemplate<String, String, String>(keyspace, "OPSData", StringSerializer.get(), StringSerializer.get(), StringSerializer.get());

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

    public static Keyspace getKeyspace() {
        return keyspace;
    }

    public static ColumnFamilyTemplate<String, String> getConceptsTemplate() {
        return conceptsTemplate;
    }

    public static SuperCfTemplate<String, String, String> getSPODataTemplate() {
        return spoDataTemplate;
    }

    public static ColumnFamilyTemplate<String, String> getInstanceDataTemplate() {
        return instanceDataTemplate;
    }

    public static SuperCfTemplate<String, String, String> getOPSDataTemplate() {
        return opsDataTemplate;
    }
}
