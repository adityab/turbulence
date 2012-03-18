package com.turbulence.core.actions;

import java.util.logging.Logger;

import java.net.URI;
import java.net.URISyntaxException;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Queue;
import java.util.LinkedList;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.*;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.kernel.Traversal;

import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.*;
import org.semanticweb.owlapi.reasoner.OWLReasoner;

import com.clarkparsia.pellet.owlapiv3.*;

import com.turbulence.core.*;
import com.turbulence.util.*;

public class RegisterSchemaAction implements Action {
    private Logger logger;
    private URI schemaURI;
    private OWLOntologyManager ontologyManager;
    private OntologyMapper ontologyMapper;
    private ClusterSpace cs;
    private Index<Node> ontologyIndex;
    private static final String KNOWN_ONTOLOGY_KEY = "KNOWN_ONTOLOGY";

    protected RegisterSchemaAction(URI uri)
    {
        logger = Logger.getLogger(this.getClass().getName());
        schemaURI = uri;
        ontologyManager = OWLManager.createOWLOntologyManager();
        ontologyMapper = new OntologyMapper(TurbulenceDriver.getOntologyStoreDirectory());
        ontologyManager.addIRIMapper(ontologyMapper);
    }

    public Result perform() {
        cs = TurbulenceDriver.getClusterSpace();
        ontologyIndex = cs.index().forNodes("ontologyIndex");

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

            Node ontNode;
            Transaction tx = cs.beginTx();
            try {
                // create a Node for the ontology itself
                ontNode = cs.createNode();
                ontNode.setProperty("IRI", iri.toString());
                // put if absent will return the OLD node if one existed, so
                // that we don't have duplicates
                Node previous = ontologyIndex.putIfAbsent(ontNode, KNOWN_ONTOLOGY_KEY, iri.toString());

                if (previous != null) {
                    // abort this entire RegisterSchema operation, because
                    // somebody else already inserted this schema before, or
                    // is doing/will do so in parallel right now.
                    tx.finish();
                    Result r = new Result();
                    r.success = false;
                    r.error = TurbulenceError.SCHEMA_IS_ALREADY_REGISTERED;
                    return r;
                }

                getKnownOntologiesReferenceNode().createRelationshipTo(ontNode, ClusterSpace.InternalRelTypes.KNOWN_ONTOLOGY);
                tx.success();
            } finally {
                tx.finish();
            }

            OWLReasoner reasoner = PelletReasonerFactory.getInstance().createReasoner(ont);
            tx = cs.beginTx();
            try {
                for (OWLClass c : ont.getClassesInSignature(false /*exclude imports closure*/)) {
                    Node cNode = link(c, reasoner);
                    if (cNode != null)
                        cNode.createRelationshipTo(ontNode, ClusterSpace.InternalRelTypes.SOURCE_ONTOLOGY);
                }

                for (OWLAxiom c : ont.getAxioms(AxiomType.OBJECT_PROPERTY_DOMAIN)) {
                    OWLObjectPropertyDomainAxiom ax = (OWLObjectPropertyDomainAxiom) c;
                    if (ax.getProperty().isAnonymous())
                        continue;
                    if (ax.getDomain().isAnonymous())
                        continue;
                    for (OWLClassExpression range : ax.getProperty().getRanges(ont)) {
                        if (range.isAnonymous())
                            continue;
                        createObjectPropertyRelationship(iri, ax.getProperty().asOWLObjectProperty(), ax.getDomain().asOWLClass(), range.asOWLClass());
                    }
                }

                for (OWLAxiom c : ont.getAxioms(AxiomType.DATA_PROPERTY_DOMAIN)) {
                    OWLDataPropertyDomainAxiom ax = (OWLDataPropertyDomainAxiom) c;
                    if (ax.getProperty().isAnonymous())
                        continue;
                    if (ax.getDomain().isAnonymous())
                        continue;
                    for (OWLDataRange range : ax.getProperty().getRanges(ont)) {
                        createDataProperty(iri, ax.getProperty().asOWLDataProperty(), ax.getDomain().asOWLClass(), range);
                    }
                }
                tx.success();
            } finally {
                tx.finish();
                // TODO handle error
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
        Traverser trav = X.traverse(Traverser.Order.BREADTH_FIRST, StopEvaluator.DEPTH_ONE, ReturnableEvaluator.ALL_BUT_START_NODE, ClusterSpace.PublicRelTypes.IS_A, Direction.INCOMING);
        return trav.getAllNodes();
    }

    private Collection<Node> superclasses(Node X) {
        Traverser trav = X.traverse(Traverser.Order.BREADTH_FIRST, StopEvaluator.DEPTH_ONE, ReturnableEvaluator.ALL_BUT_START_NODE, ClusterSpace.PublicRelTypes.IS_A, Direction.OUTGOING);
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
                    org.neo4j.graphdb.traversal.Traverser linkTrav = Traversal.description()
                        .breadthFirst()
                        .evaluator(Evaluators.atDepth(1))
                        .evaluator(Evaluators.returnWhereEndNodeIs(X))
                        .relationships(ClusterSpace.PublicRelTypes.IS_A, Direction.OUTGOING)
                        .traverse(XC);

                    Relationship rel = linkTrav.relationships().iterator().next();
                    rel.delete();
                    XC.createRelationshipTo(N, ClusterSpace.PublicRelTypes.IS_A);
                }
            }

            N.createRelationshipTo(X, ClusterSpace.PublicRelTypes.IS_A);
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
            X.createRelationshipTo(N, ClusterSpace.PublicRelTypes.IS_A);

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
                    XS.createRelationshipTo(N, ClusterSpace.PublicRelTypes.IS_A);

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
        Traverser trav = ref.traverse(Traverser.Order.BREADTH_FIRST, StopEvaluator.END_OF_GRAPH, ReturnableEvaluator.ALL_BUT_START_NODE, ClusterSpace.InternalRelTypes.ROOT, Direction.OUTGOING);
        return trav.getAllNodes();
    }

    private void addRoot(Node n) {
        if (n.hasRelationship(ClusterSpace.InternalRelTypes.ROOT, Direction.INCOMING))
            return;
        Node ref = cs.getReferenceNode();
        ref.createRelationshipTo(n, ClusterSpace.InternalRelTypes.ROOT);
    }

    private boolean isRoot(Node n) {
        return n.hasRelationship(ClusterSpace.InternalRelTypes.ROOT, Direction.INCOMING);
    }

    private void removeRoot(Node n) {
        assert isRoot(n);
        Relationship rel = n.getSingleRelationship(ClusterSpace.InternalRelTypes.ROOT, Direction.INCOMING);
        rel.delete();
    }

    private void addEquivalentClassLink(Node from, Node to) {
        from.createRelationshipTo(to, ClusterSpace.PublicRelTypes.EQUIVALENT_CLASS);
    }

    private Node getKnownOntologiesReferenceNode() {
        Node ref = cs.getReferenceNode();
        Relationship ko = ref.getSingleRelationship(ClusterSpace.InternalRelTypes.ONTOLOGIES_REFERENCE, Direction.OUTGOING);
        if (ko == null) {
            Transaction tx = cs.beginTx();
            try {
                Node ontRef = cs.createNode();
                ref.createRelationshipTo(ontRef, ClusterSpace.InternalRelTypes.ONTOLOGIES_REFERENCE);
                tx.success();
                return ontRef;
            } finally {
                tx.finish();
            }
        }
        return ko.getEndNode();
    }

    private Node getClassNode(IRI ontologyIRI, OWLClass clazz) {
        IRI iri = clazz.getIRI();
        Index<Node> index = cs.index().forNodes("ontologyIndex");
        Node ontNode = index.get(KNOWN_ONTOLOGY_KEY, ontologyIRI).getSingle();
        if (ontNode == null)
            return null;

        final IRI clazzIRI = clazz.getIRI();
        Traverser trav = ontNode.traverse(Traverser.Order.BREADTH_FIRST, StopEvaluator.DEPTH_ONE, new ReturnableEvaluator() {
            public boolean isReturnableNode(TraversalPosition pos) {
                return pos.notStartNode() && pos.currentNode().getProperty("IRI").equals(clazzIRI.toString());
            }
        }, ClusterSpace.InternalRelTypes.SOURCE_ONTOLOGY, Direction.INCOMING);

        Iterator<Node> iter = trav.iterator();
        return iter.hasNext() ? iter.next() : null;
    }

    private Node link(OWLClass c, OWLReasoner r) {
        // shouldn't create a new Node if a ndoe for c already exists
        // FIXME
        Queue<Node> queue = new LinkedList<Node>();
        for (Node root : getRoots()) {
            queue.add(root);
        }
        Node n = null;
        n = cs.createNode();
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

        // no roots can be subclasses of something else
        logger.warning("Roots len " + getRoots().size());
        for (Node R : getRoots())
            assert !R.hasRelationship(ClusterSpace.PublicRelTypes.IS_A, Direction.OUTGOING);

        return n;
    }

    private void createObjectPropertyRelationship(IRI ontologyIRI, OWLObjectProperty property, OWLClass domain, OWLClass range) {
        Node domainNode = getClassNode(ontologyIRI, domain);
        if (domainNode == null)
            return;
        Node rangeNode = getClassNode(ontologyIRI, range);
        if (rangeNode == null)
            return;

        Transaction tx = cs.beginTx();
        try {
            // TODO set column family location on domain
            Relationship rel = domainNode.createRelationshipTo(rangeNode, ClusterSpace.PublicRelTypes.OBJECT_RELATIONSHIP);
            rel.setProperty("IRI", property.getIRI().toString());
            tx.success();
        } finally {
            tx.finish();
        }
    }

    private void createDataProperty(IRI ontologyIRI, OWLDataProperty property, OWLClass domain, OWLDataRange range) {
        Node domainNode = getClassNode(ontologyIRI, domain);
        if (domainNode == null)
            return;

        Transaction tx = cs.beginTx();
        try {
            // TODO set to column family location
            //domainNode.setProperty(property.getIRI().toString());
            tx.success();
        } finally {
            tx.finish();
        }
    }

}
