package com.turbulence.core;

import java.util.logging.Logger;

import java.util.Collection;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.kernel.EmbeddedGraphDatabase;

public class ClusterSpace {
    private static final String INTERNAL_TURBULENCE_IRI = "http://turbulencedb.com/internal/";
    private static final String TURBULENCE_IRI = "http://turbulencedb.com/definitions/";
    private GraphDatabaseService db;
    private Logger logger;

    public static enum InternalRelTypes implements RelationshipType {
        ROOT                      ("Root"), // a ROOT goes from reference Node
                                            // (outgoing) -> to Node
        ROOT_OBJECT_PROPERTY      ("RootObjectProperty"),
        SOURCE_ONTOLOGY           ("sourceOntology"),
        KNOWN_ONTOLOGY            ("knownOntology"),
        ONTOLOGIES_REFERENCE      ("ontologiesReference");

        private final String iriName;
        InternalRelTypes(String iriName) {
            this.iriName = iriName;
        }

        public String getIRI() {
            return INTERNAL_TURBULENCE_IRI + iriName;
        }
    }

    public static enum PublicRelTypes implements RelationshipType {
        IS_A                      ("subClassOf"),
        EQUIVALENT_CLASS          ("equivalentClass"),
        OBJECT_RELATIONSHIP       ("objectRelationship"),
        DATATYPE_RELATIONSHIP     ("datatypeRelationship"),
        EQUIVALENT_OBJECT_PROPERTY("equivalentObjectProperty");

        private final String iriName;
        PublicRelTypes(String iriName) {
            this.iriName = iriName;
        }

        public String getIRI() {
            if (this == IS_A)
                return "http://www.w3.org/2000/01/rdf-schema#" + iriName;
            else if (this == EQUIVALENT_CLASS)
                return "http://www.w3.org/2002/07/owl#" + iriName;

            return TURBULENCE_IRI + iriName;
        }
    }

    public ClusterSpace(String dbPath) {
        logger = Logger.getLogger(this.getClass().getName());
        logger.info("ClusterSpace using database " + dbPath);
        db = new EmbeddedGraphDatabase(dbPath);
        registerShutdownHook(db);

    }

    public Collection<Node> getEquivalentClasses(Node clazz) {
        return clazz.traverse(Traverser.Order.BREADTH_FIRST, StopEvaluator.DEPTH_ONE, ReturnableEvaluator.ALL_BUT_START_NODE, PublicRelTypes.EQUIVALENT_CLASS, Direction.BOTH).getAllNodes();
    }

    private static void registerShutdownHook(final GraphDatabaseService db) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                System.out.println("Shutting down DB");
                db.shutdown();
            }
        });
    }

    /* all these are delegate methods */

	public Node createNode() {
		return db.createNode();
	}

	public Node getNodeById(long id) {
		return db.getNodeById(id);
	}

	public Relationship getRelationshipById(long id) {
		return db.getRelationshipById(id);
	}

	public Node getReferenceNode() {
		return db.getReferenceNode();
	}

    @Deprecated
	public Iterable<RelationshipType> getRelationshipTypes() {
		return db.getRelationshipTypes();
	}

	public void shutdown() {
		db.shutdown();
	}

	public Transaction beginTx() {
		return db.beginTx();
	}

	public <T> TransactionEventHandler<T> registerTransactionEventHandler(
			TransactionEventHandler<T> handler) {
		return db.registerTransactionEventHandler(handler);
	}

	public <T> TransactionEventHandler<T> unregisterTransactionEventHandler(
			TransactionEventHandler<T> handler) {
		return db.unregisterTransactionEventHandler(handler);
	}

	public KernelEventHandler registerKernelEventHandler(
			KernelEventHandler handler) {
		return db.registerKernelEventHandler(handler);
	}

	public KernelEventHandler unregisterKernelEventHandler(
			KernelEventHandler handler) {
		return db.unregisterKernelEventHandler(handler);
	}

	public IndexManager index() {
		return db.index();
	}
}
