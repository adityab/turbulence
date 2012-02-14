package com.turbulence.core.actions;

import java.util.logging.Logger;

import java.net.URI;
import java.net.URISyntaxException;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.Queue;
import java.util.LinkedList;

import org.neo4j.graphdb.*;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.util.FileUtils;

import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.*;
import org.semanticweb.owlapi.reasoner.OWLReasoner;
import org.semanticweb.owlapi.reasoner.NodeSet;

import com.clarkparsia.pellet.owlapiv3.*;

import com.turbulence.core.*;
import com.turbulence.util.*;

public class RegisterSchemaAction {
    public enum RelTypes implements RelationshipType {
        ROOT, // a ROOT goes from reference Node (outgoing) -> to Node
        IS_A,
        EQUIVALENT_CLASS,
        SOURCE_ONTOLOGY,
        KNOWN_ONTOLOGY,
        ONTOLOGIES_REFERENCE
    }

    private Logger logger;
    private URI schemaURI;
    private OWLOntologyManager ontologyManager;
    private OntologyMapper ontologyMapper;
    private EmbeddedGraphDatabase cs;

    protected RegisterSchemaAction(URI uri)
    {
        logger = Logger.getLogger(this.getClass().getName());
        schemaURI = uri;
        ontologyManager = OWLManager.createOWLOntologyManager();
        ontologyMapper = new OntologyMapper(TurbulenceDriver.getOntologyStoreDirectory());
        ontologyManager.addIRIMapper(ontologyMapper);
    }

    public Result perform() {
        cs = TurbulenceDriver.getClusterSpaceDB();
        registerShutdownHook(cs);

        logger.warning(this.getClass().getName()+ " perform "+ schemaURI);
        IRI iri = IRI.create(schemaURI);
        // TODO keep an index of known registered ontologies to avoid
        // duplication
        // ship buggy similarity ontology
        try {
            OWLOntology ont = ontologyManager.loadOntology(iri);
            if (ontologyMapper.getDocumentIRI(iri) == null)
                TurbulenceDriver.submit(new OntologySaver(iri, ont, TurbulenceDriver.getOntologyStoreDirectory()));

            for (OWLImportsDeclaration decl : ont.getImportsDeclarations()) {
                logger.info("Registering imported ontology " + decl.getIRI());
                try {
                    RegisterSchemaAction act = ActionFactory.createRegisterSchemaAction(new URI(decl.getIRI().toString()));
                    act.perform();
                } catch (URISyntaxException e) {} // seriously?
            }
            logger.warning("loaded " + ont);

            PelletReasoner reasoner = PelletReasonerFactory.getInstance().createReasoner(ont);

            for (OWLClass c : ont.getClassesInSignature(false /*exclude imports closure*/)) {
                link(c, reasoner);
            }

            Result r = new Result();
            r.success = true;
            r.message = "yeayayay";
            return r;
        } catch (OWLOntologyCreationException e) {
            // TODO handle this doo doo
            logger.warning("OOCE");
            Result r = new Result();
            r.success = false;
            r.error = TurbulenceError.ONTOLOGY_CREATION_FAILED;
            r.message = e.getMessage();
            return r;
        }
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
        Traverser trav = X.traverse(Traverser.Order.BREADTH_FIRST, StopEvaluator.DEPTH_ONE, ReturnableEvaluator.ALL_BUT_START_NODE, RelTypes.IS_A, Direction.INCOMING);
        return trav.getAllNodes();
    }

    private Collection<Node> superclasses(Node X) {
        Traverser trav = X.traverse(Traverser.Order.BREADTH_FIRST, StopEvaluator.DEPTH_ONE, ReturnableEvaluator.ALL_BUT_START_NODE, RelTypes.IS_A, Direction.OUTGOING);
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
                    Relationship rel = XC.getSingleRelationship(RelTypes.IS_A, Direction.OUTGOING);
                    rel.delete();
                    XC.createRelationshipTo(N, RelTypes.IS_A);
                }
            }

            N.createRelationshipTo(X, RelTypes.IS_A);
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
            X.createRelationshipTo(N, RelTypes.IS_A);

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
                    XS.createRelationshipTo(N, RelTypes.IS_A);

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
        Node ref = cs.getReferenceNode();
        Traverser trav = ref.traverse(Traverser.Order.BREADTH_FIRST, StopEvaluator.END_OF_GRAPH, ReturnableEvaluator.ALL_BUT_START_NODE, RelTypes.ROOT, Direction.OUTGOING);
        return trav.getAllNodes();
    }

    private void addRoot(Node n) {
        if (n.hasRelationship(RelTypes.ROOT, Direction.INCOMING))
            return;
        Node ref = cs.getReferenceNode();
        ref.createRelationshipTo(n, RelTypes.ROOT);
    }

    private boolean isRoot(Node n) {
        return n.hasRelationship(RelTypes.ROOT, Direction.INCOMING);
    }

    private void removeRoot(Node n) {
        assert isRoot(n);
        Relationship rel = n.getSingleRelationship(RelTypes.ROOT, Direction.INCOMING);
        rel.delete();
    }

    private void addEquivalentClassLink(Node from, Node to) {
        from.createRelationshipTo(to, RelTypes.EQUIVALENT_CLASS);
    }

    public void link(OWLClass c, OWLReasoner r) {
        // shouldn't create a new Node if a ndoe for c already exists
        // FIXME
        Queue<Node> queue = new LinkedList<Node>();
        for (Node root : getRoots()) {
            queue.add(root);
        }
        Transaction tx = cs.beginTx();
        try {
            Node n = cs.createNode();
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
            assert !R.hasRelationship(RelTypes.IS_A, Direction.OUTGOING);
    }

    private static void registerShutdownHook(final GraphDatabaseService cs) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                System.out.println("Shutting down cs");
                cs.shutdown();
            }
        });
    }
}
