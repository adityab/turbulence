package com.turbulence.core;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.collections.iterators.*;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ReturnableEvaluator;
import org.neo4j.graphdb.StopEvaluator;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.TraversalPosition;
import org.neo4j.graphdb.Traverser;
import org.neo4j.kernel.Traversal;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.TripleMatch;
import com.hp.hpl.jena.graph.impl.GraphBase;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.util.iterator.NiceIterator;
import com.hp.hpl.jena.query.QueryExecException;

import com.turbulence.core.ClusterSpace;

class ClusterSpaceJenaIterator extends NiceIterator<Triple> {
    private static final Log logger =
        LogFactory.getLog(ClusterSpaceJenaIterator.class);
    private final Iterator<org.neo4j.graphdb.Relationship> internal;
    /**
     * Constructs a new instance.
     */
    public ClusterSpaceJenaIterator(Iterator<org.neo4j.graphdb.Relationship> it)
    {
        internal = it;
    }

	public boolean hasNext() {
		return internal.hasNext();
	}

	public Triple next() {
	    org.neo4j.graphdb.Relationship rel = internal.next();

	    Node sub = Node.createURI((String)rel.getStartNode().getProperty("IRI", "bombaderror"));

	    Node pred;
	    if (rel.hasProperty("IRI")) {
	        pred = Node.createURI((String) rel.getProperty("IRI"));
        }
        else {
            RelationshipType type = rel.getType();
            try {
                pred = Node.createURI(ClusterSpace.PublicRelTypes.valueOf(type.name()).getIRI());
            } catch (IllegalArgumentException e) {
                try {
                    pred = Node.createURI(ClusterSpace.InternalRelTypes.valueOf(type.name()).getIRI());
                } catch (IllegalArgumentException ae) {
                    throw new RuntimeException("Unknown relationship type " + type.name());
                }
            }
        }

        Node obj = Node.createURI((String)rel.getEndNode().getProperty("IRI", "bombaderror"));
        return Triple.create(sub, pred, obj);
	}
}

public class ClusterSpaceJenaGraph extends GraphBase {
    private static final Log logger =
        LogFactory.getLog(ClusterSpaceJenaGraph.class);

    private ClusterSpace cs;

    /**
     * Constructs a new instance.
     */
    public ClusterSpaceJenaGraph()
    {
        cs = TurbulenceDriver.getClusterSpace();
    }

    private org.neo4j.graphdb.Node getClass(final String classIRI) {
        // find the object in the cluster space
        // TODO: abstract this
        String ontologyIRI;
        if (classIRI.lastIndexOf('#') != -1) {
            ontologyIRI = classIRI.substring(0, classIRI.lastIndexOf('#'));
        }
        else {
            ontologyIRI = classIRI.substring(0, classIRI.lastIndexOf('/')+1);
        }
        Index<org.neo4j.graphdb.Node> ontologyIndex = cs.index().forNodes("ontologyIndex");
        org.neo4j.graphdb.Node ont = ontologyIndex.get("KNOWN_ONTOLOGY", ontologyIRI).getSingle();
        if (ont == null)
            return null;
        // get the class
        Traverser trav = ont.traverse(Traverser.Order.BREADTH_FIRST, StopEvaluator.DEPTH_ONE, new ReturnableEvaluator() {
            public boolean isReturnableNode(TraversalPosition pos) {
                return ((String)pos.currentNode().getProperty("IRI")).equals(classIRI.toString());
            }
        }, ClusterSpace.InternalRelTypes.SOURCE_ONTOLOGY, Direction.INCOMING);

        if (trav.iterator().hasNext())
            return trav.iterator().next();

        return null;
    }

    private ClusterSpaceJenaIterator allClasses() {
        List<Iterator<Relationship>> rootIterators = new ArrayList<Iterator<Relationship>>();
        for (org.neo4j.graphdb.Node root : Traversal.description()
                .breadthFirst()
                .evaluator(Evaluators.atDepth(1))
                .relationships(ClusterSpace.InternalRelTypes.ROOT)
                .traverse(cs.getReferenceNode())
                .nodes()) {
            org.neo4j.graphdb.traversal.Traverser trav = Traversal.description()
                .breadthFirst()
                .evaluator(Evaluators.all())
                .relationships(ClusterSpace.PublicRelTypes.IS_A, Direction.INCOMING)
                .relationships(ClusterSpace.PublicRelTypes.EQUIVALENT_CLASS, Direction.BOTH)
                .traverse(root);
            rootIterators.add(trav.relationships().iterator());
        }

        return new ClusterSpaceJenaIterator(IteratorUtils.chainedIterator(rootIterators));
    }

    // class and all subclasses
    private ClusterSpaceJenaIterator classCover(final String classIRI) {
        List<Iterator<Relationship>> subclassIterators = new ArrayList<Iterator<Relationship>>();
        org.neo4j.graphdb.Node cNode = getClass(classIRI);
        if (cNode == null)
            return new ClusterSpaceJenaIterator(EmptyIterator.INSTANCE);

        TraversalDescription desc = Traversal.description()
                                    .depthFirst()
                                    .evaluator(Evaluators.all())
                                    .relationships(ClusterSpace.PublicRelTypes.IS_A, Direction.INCOMING)
                                    .relationships(ClusterSpace.PublicRelTypes.EQUIVALENT_CLASS, Direction.BOTH);

        return new ClusterSpaceJenaIterator(desc.traverse(cNode).relationships().iterator());
    }

    protected ExtendedIterator<Triple> graphBaseFind(TripleMatch tm) {
        Triple triple = tm.asTriple();
        Node sub  = triple.getSubject();
        Node pred = triple.getPredicate();
        Node obj  = triple.getObject();
        logger.warn("graphBaseFind: " + sub + " -- " + pred + " -- " + obj);

        TraversalDescription trav = Traversal.description()
                                    .breadthFirst()
                                    .evaluator(Evaluators.all())
                                    .relationships(ClusterSpace.InternalRelTypes.ROOT);

        // this evaluator allows us to traverse from the reference node,
        // and follow 'ROOT' relationships, but not include the relationships
        // (and the reference node) themselves in the results
        trav = trav.evaluator( Evaluators.lastRelationshipTypeIs(
                    Evaluation.EXCLUDE_AND_CONTINUE,
                    Evaluation.INCLUDE_AND_CONTINUE,
                    ClusterSpace.InternalRelTypes.ROOT));

        org.neo4j.graphdb.Node startNode = null;
        Direction relationshipDirection;

        if (sub.isURI()) {
            startNode = getClass(sub.getURI());
            if (startNode == null)
                return new ClusterSpaceJenaIterator(EmptyIterator.INSTANCE);

            relationshipDirection = Direction.OUTGOING;

            trav = trav.evaluator(Evaluators.atDepth(1));

            if (obj.isURI()) {
                org.neo4j.graphdb.Node objNode = getClass(obj.getURI());
                if (objNode == null)
                    return new ClusterSpaceJenaIterator(EmptyIterator.INSTANCE);

                trav = trav.evaluator(Evaluators.returnWhereEndNodeIs(objNode));
                for (org.neo4j.graphdb.Node equiv : cs.getEquivalentClasses(objNode))
                    trav = trav.evaluator(Evaluators.returnWhereEndNodeIs(equiv));
            }
        }
        else if (obj.isURI()) {
            startNode = getClass(obj.getURI());
            if (startNode == null)
                return new ClusterSpaceJenaIterator(EmptyIterator.INSTANCE);

            relationshipDirection = Direction.INCOMING;

            trav = trav.evaluator(Evaluators.atDepth(1));

            if (sub.isURI()) {
                org.neo4j.graphdb.Node subNode = getClass(sub.getURI());
                if (subNode == null)
                    return new ClusterSpaceJenaIterator(EmptyIterator.INSTANCE);

                trav = trav.evaluator(Evaluators.returnWhereEndNodeIs(subNode));
                for (org.neo4j.graphdb.Node equiv : cs.getEquivalentClasses(subNode))
                    trav = trav.evaluator(Evaluators.returnWhereEndNodeIs(equiv));
            }
        }
        else {
            throw new QueryExecException("TODO: both undefined");
        }

        if (pred.isURI()
            && pred.getURI().equals("http://www.w3.org/2000/01/rdf-schema#subClassOf")) {
            trav = trav.relationships(ClusterSpace.PublicRelTypes.IS_A, relationshipDirection);
            trav = trav.relationships(ClusterSpace.PublicRelTypes.EQUIVALENT_CLASS); // equivalent class never has direction
        }
        else if (pred.isURI()) { /* custom relationship */
            throw new QueryExecException("TODO: implement custom relationship evaluator");
        }
        else if (pred == Node.ANY) {
            for (ClusterSpace.PublicRelTypes type : ClusterSpace.PublicRelTypes.values())
                trav = trav.relationships(type, relationshipDirection);
        }
        else {
            throw new QueryExecException("Unknown relationship type");
        }

        return new ClusterSpaceJenaIterator(trav.traverse(startNode).relationships().iterator());
    }
}
