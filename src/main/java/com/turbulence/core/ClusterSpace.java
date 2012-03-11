package com.turbulence.core;

import java.util.logging.Logger;

import java.util.Collection;
import java.util.HashSet;
import java.util.Queue;
import java.util.LinkedList;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.kernel.EmbeddedGraphDatabase;

import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.semanticweb.owlapi.reasoner.OWLReasoner;

public class ClusterSpace {
    private static final String INTERNAL_TURBULENCE_IRI = "http://turbulencedb.com/internal/";
    private static final String TURBULENCE_IRI = "http://turbulencedb.com/definitions/";
    private GraphDatabaseService db;
    private Logger logger;

    public static enum InternalRelTypes implements RelationshipType {
        ROOT                      ("Root"), // a ROOT goes from reference Node
                                            // (outgoing) -> to Node
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
        DATATYPE_RELATIONSHIP     ("datatypeRelationship");

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

    private Collection<Node> getSiblings(Node N) {
        Collection<Node> siblings = new HashSet<Node>();
        for (Node parent : superclasses(N)) {
            for (Node child : subclasses(parent)) {
                if (child != N)
                    siblings.add(child);
            }
        }
        return siblings;
    }

    private Collection<Node> subclasses(Node X) {
        Traverser trav = X.traverse(Traverser.Order.BREADTH_FIRST, StopEvaluator.DEPTH_ONE, ReturnableEvaluator.ALL_BUT_START_NODE, PublicRelTypes.IS_A, Direction.INCOMING);
        return trav.getAllNodes();
    }

    private Collection<Node> superclasses(Node X) {
        Traverser trav = X.traverse(Traverser.Order.BREADTH_FIRST, StopEvaluator.DEPTH_ONE, ReturnableEvaluator.ALL_BUT_START_NODE, PublicRelTypes.IS_A, Direction.OUTGOING);
        return trav.getAllNodes();
    }

    /**
     * Compares two Nodes of type owlclass (with associated IRI)
     * using the Reasoner and modifies the cluster space
     */
    private boolean compareClasses(Node N, Node X, OWLReasoner r) {
        System.out.println("CCCCCalled");
        OWLOntology ont = r.getRootOntology();
        OWLOntologyManager man = ont.getOWLOntologyManager();

        OWLClass Nclass = man.getOWLDataFactory().getOWLClass(IRI.create((String)N.getProperty("IRI")));
        OWLClass Xclass = man.getOWLDataFactory().getOWLClass(IRI.create((String)X.getProperty("IRI")));
        System.out.println("Comparing " + Nclass + " and " + Xclass);

        // since we've to anyway directly proceed by checking for membership of
        // X in subclass/superclasses of N, we should seriously optimize this
        // to directly proceed from the superclass/subclass chain on N and
        // their positions in the cluster space
        if (r.getEquivalentClasses(Nclass).contains(Xclass)) {
            System.out.println("Equivalent!");
            addEquivalentClassLink(X, N);
            return true;
        }
        else if (r.getSuperClasses(Nclass, true /*direct*/).containsEntity(Xclass)) {
            System.out.println(Nclass + " is a subclass of " + Xclass);
            for (Node XC : subclasses(X)) {
                OWLClass XCclass = man.getOWLDataFactory().getOWLClass(IRI.create((String)XC.getProperty("IRI")));
                if (r.getEquivalentClasses(Nclass).contains(XCclass)) {
                    addEquivalentClassLink(XC, N);
                    return true;
                }
                else if (r.getSubClasses(Nclass, true /*direct*/).containsEntity(XCclass)) {
                    Relationship rel = XC.getSingleRelationship(PublicRelTypes.IS_A, Direction.OUTGOING);
                    rel.delete();
                    XC.createRelationshipTo(N, PublicRelTypes.IS_A);
                }
            }

            N.createRelationshipTo(X, PublicRelTypes.IS_A);
            // we are ready to return true as soon as we're done with checking
            // the siblings

            // deal with siblings
            Collection<Node> siblings;
            if (isRoot(X)) {
                siblings = getRoots();
                siblings.remove(X);
            }
            else {
                siblings = getSiblings(X);
            }

            for (Node XS : siblings) {
                OWLClass XSclass = man.getOWLDataFactory().getOWLClass(IRI.create((String)XS.getProperty("IRI")));
                if (r.getSuperClasses(Nclass, true /*direct*/).containsEntity(XSclass)) {
                    compareClasses(N, XS, r);
                }
            }

            assert !isRoot(N);
            return true;
        }
        else if (r.getSubClasses(Nclass, true /*direct*/).containsEntity(Xclass)) {
            System.out.println(Nclass + " is a superclass of " + Xclass);
            X.createRelationshipTo(N, PublicRelTypes.IS_A);

            // deal with siblings
            Collection<Node> siblings;
            if (isRoot(X)) {
                System.out.println(Xclass + " is a ROOT");
                siblings = getRoots();
                siblings.remove(X);
            }
            else {
                siblings = getSiblings(X);
            }

            for (Node XS : siblings) {
                OWLClass XSclass = man.getOWLDataFactory().getOWLClass(IRI.create((String)XS.getProperty("IRI")));
                if (r.getSubClasses(Nclass, true /*direct*/).containsEntity(XSclass)) {
                    XS.createRelationshipTo(N, PublicRelTypes.IS_A);

                    if (isRoot(XS)) {
                        removeRoot(XS);
                        addRoot(N);
                    }
                    assert !isRoot(XS);
                }
            }

            if (isRoot(X)) {
                removeRoot(X);
                addRoot(N);
            }
            assert !isRoot(X);
            return true;
        }

        return false;
    }

    private Collection<Node> getRoots() {
        Node ref = db.getReferenceNode();
        Traverser trav = ref.traverse(Traverser.Order.BREADTH_FIRST, StopEvaluator.END_OF_GRAPH, ReturnableEvaluator.ALL_BUT_START_NODE, InternalRelTypes.ROOT, Direction.OUTGOING);
        return trav.getAllNodes();
    }

    private void addRoot(Node n) {
        Node ref = db.getReferenceNode();
        ref.createRelationshipTo(n, InternalRelTypes.ROOT);
    }

    private boolean isRoot(Node n) {
        return n.hasRelationship(InternalRelTypes.ROOT, Direction.BOTH);
    }

    private void removeRoot(Node n) {
        assert isRoot(n);
        Relationship rel = n.getSingleRelationship(InternalRelTypes.ROOT, Direction.BOTH);
        rel.delete();
    }

    private void addEquivalentClassLink(Node from, Node to) {
        from.createRelationshipTo(to, PublicRelTypes.EQUIVALENT_CLASS);
    }

    public void link(OWLClass c, OWLReasoner r) {
        // shouldn't create a new Node if a ndoe for c already exists
        // FIXME
        Queue<Node> queue = new LinkedList<Node>();
        for (Node root : getRoots()) {
            queue.add(root);
        }
        Transaction tx = db.beginTx();
        try {
            Node n = db.createNode();
            n.setProperty("IRI", c.getIRI().toString());

            boolean linked = false;
            while (!queue.isEmpty()) {
                Node x = queue.remove();
                linked = compareClasses(n, x, r);
                if (!linked) {
                    for (Node sub : subclasses(x))
                        queue.add(sub);
                }
                else {
                    break;
                }
            }

            if (!linked) {
                System.out.println("No links for " + c.getIRI());
                addRoot(n);
            }

            tx.success();
        } finally {
            tx.finish();
        }

        // no roots can be subclasses of something else
        System.out.println("Roots len " + getRoots().size());
        for (Node R : getRoots())
            assert !R.hasRelationship(PublicRelTypes.IS_A, Direction.OUTGOING);
    }

    private static void registerShutdownHook(final GraphDatabaseService db) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                System.out.println("Shutting down DB");
                db.shutdown();
            }
        });
    }

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

	public Iterable<Node> getAllNodes() {
		return db.getAllNodes();
	}

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
