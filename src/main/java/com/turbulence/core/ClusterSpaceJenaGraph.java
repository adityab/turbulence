package com.turbulence.core;

import java.net.URI;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.index.Index;

import org.neo4j.graphdb.ReturnableEvaluator;
import org.neo4j.graphdb.StopEvaluator;

import org.neo4j.graphdb.TraversalPosition;

import org.neo4j.graphdb.Traverser;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.TripleMatch;
import com.hp.hpl.jena.graph.impl.GraphBase;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.util.iterator.NiceIterator;

import com.turbulence.core.actions.RegisterSchemaAction;

class ClusterSpaceJenaIterator extends NiceIterator<Triple> {
    private final Iterator<org.neo4j.graphdb.Node> internal;
    /**
     * Constructs a new instance.
     */
    public ClusterSpaceJenaIterator(Iterator<org.neo4j.graphdb.Node> it)
    {
        internal = it;
    }

	public boolean hasNext() {
		return internal.hasNext();
	}

	public Triple next() {
	    org.neo4j.graphdb.Node n = internal.next();
	    // TODO: should actually return proper SVO
        return Triple.create(Node.createURI((String)n.getProperty("IRI")),
                             Node.createURI("http://www.w3.org/2000/01/rdf-schema#subClassOf"),
                             Node.createURI("http://purl.org/dc/terms/Agent"));
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

    protected ExtendedIterator<Triple> graphBaseFind(TripleMatch triple) {
        logger.warn(triple.getMatchSubject() + " " + triple.getMatchPredicate() + " " + triple.getMatchObject());

        Node pred = triple.getMatchPredicate();

        if (pred.isURI()
            && pred.getURI().equals("http://www.w3.org/2000/01/rdf-schema#subClassOf")) {
            logger.warn("IS_A");

            Node obj = triple.getMatchObject();
            assert obj.isURI();

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
            }, RegisterSchemaAction.RelTypes.SOURCE_ONTOLOGY, Direction.INCOMING);

            assert trav.getAllNodes().size() == 1;

            org.neo4j.graphdb.Node cNode = trav.iterator().next();

            // process IS_A
            Traverser isATrav = cNode.traverse(Traverser.Order.BREADTH_FIRST, 
                    StopEvaluator.END_OF_GRAPH,
                    ReturnableEvaluator.ALL,
                    RegisterSchemaAction.RelTypes.IS_A, Direction.INCOMING,
                    RegisterSchemaAction.RelTypes.EQUIVALENT_CLASS, Direction.BOTH);

            return new ClusterSpaceJenaIterator(isATrav.iterator());
        }

        return null;
    }
}
