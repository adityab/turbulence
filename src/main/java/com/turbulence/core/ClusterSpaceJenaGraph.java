package com.turbulence.core;

import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.collections.IteratorUtils;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.index.Index;

import org.neo4j.graphdb.ReturnableEvaluator;
import org.neo4j.graphdb.StopEvaluator;

import org.neo4j.graphdb.traversal.Evaluators;

import org.neo4j.graphdb.TraversalPosition;
import org.neo4j.graphdb.Traverser;

import org.neo4j.kernel.Traversal;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.TripleMatch;
import com.hp.hpl.jena.graph.impl.GraphBase;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.util.iterator.NiceIterator;

import com.turbulence.core.actions.RegisterSchemaAction;

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
	    logger.warn(rel.getStartNode().getProperty("IRI") + "--" + rel.getType().name() + "--" + rel.getEndNode().getProperty("IRI"));
	    // TODO: should actually return proper SVO
        return Triple.create(Node.createURI((String)rel.getStartNode().getProperty("IRI")),
                             Node.createURI((String)rel.getProperty("IRI", rel.getType().name())), // TODO deal with IS_A
                             Node.createURI((String)rel.getEndNode().getProperty("IRI")));
	}
}

public class ClusterSpaceJenaGraph extends GraphBase {
    private static final Log logger =
        LogFactory.getLog(ClusterSpaceJenaGraph.class);

    private GraphDatabaseService cs;

    /**
     * Constructs a new instance.
     */
    public ClusterSpaceJenaGraph()
    {
        cs = TurbulenceDriver.getClusterSpaceDB();
    }

    protected ExtendedIterator<Triple> graphBaseFind(TripleMatch tm) {
        Triple triple = tm.asTriple();
        logger.warn(triple.getSubject() + " " + triple.getPredicate() + " " + triple.getObject());
        Node pred = triple.getPredicate();

        if (pred.isURI()
            && pred.getURI().equals("http://www.w3.org/2000/01/rdf-schema#subClassOf")) {
            Node obj = triple.getObject();

            if (obj == Node.ANY) {
                List<Iterator> rootIterators = new ArrayList<Iterator>();
                for (org.neo4j.graphdb.Node root : Traversal.description()
                        .breadthFirst()
                        .evaluator(Evaluators.atDepth(1))
                        .relationships(RegisterSchemaAction.InternalRelTypes.ROOT)
                        .traverse(cs.getReferenceNode())
                        .nodes()) {
                    org.neo4j.graphdb.traversal.Traverser trav = Traversal.description()
                        .breadthFirst()
                        .evaluator(Evaluators.all())
                        .relationships(RegisterSchemaAction.PublicRelTypes.IS_A, Direction.INCOMING)
                        .relationships(RegisterSchemaAction.PublicRelTypes.EQUIVALENT_CLASS, Direction.BOTH)
                        .traverse(root);
                    rootIterators.add(trav.relationships().iterator());
                }

                return new ClusterSpaceJenaIterator(IteratorUtils.chainedIterator(rootIterators));
            }
            logger.warn(obj.isConcrete() + " | " + obj.isVariable() + " | " + obj.isBlank() + " | " + obj.isLiteral() + " | " + obj.isURI() + " | " + (obj == Node.ANY));

            final String uri = obj.getURI();
            // find the object in the cluster space
            // TODO: abstract this
            String ontologyIRI;
            if (uri.lastIndexOf('#') != -1) {
                ontologyIRI = uri.substring(0, uri.lastIndexOf('#')+1);
            }
            else {
                ontologyIRI = uri.substring(0, uri.lastIndexOf('/')+1);
            }
            Index<org.neo4j.graphdb.Node> ontologyIndex = cs.index().forNodes("ontologyIndex");
            org.neo4j.graphdb.Node ont = ontologyIndex.get("KNOWN_ONTOLOGY", ontologyIRI).getSingle();
            logger.warn("ONT: " + ontologyIRI + " " + ont);
            // get the class
            Traverser trav = ont.traverse(Traverser.Order.BREADTH_FIRST, StopEvaluator.DEPTH_ONE, new ReturnableEvaluator() {
                public boolean isReturnableNode(TraversalPosition pos) {
                    return ((String)pos.currentNode().getProperty("IRI")).equals(uri);
                }
            }, RegisterSchemaAction.InternalRelTypes.SOURCE_ONTOLOGY, Direction.INCOMING);

            assert trav.getAllNodes().size() == 1;

            org.neo4j.graphdb.Node cNode = trav.iterator().next();

            // process IS_A
            org.neo4j.graphdb.traversal.Traverser isATrav = Traversal.description()
                                .breadthFirst()
                                .relationships(RegisterSchemaAction.PublicRelTypes.IS_A, Direction.INCOMING)
                                .relationships(RegisterSchemaAction.PublicRelTypes.EQUIVALENT_CLASS, Direction.BOTH)
                                .evaluator(Evaluators.all())
                                .traverse(cNode);

            return new ClusterSpaceJenaIterator(isATrav.relationships().iterator());
        }

        return null;
    }
}
