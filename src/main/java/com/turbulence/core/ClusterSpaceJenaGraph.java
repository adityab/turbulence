package com.turbulence.core;

import java.nio.ByteBuffer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.codec.digest.DigestUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.collections.iterators.*;

import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ReturnableEvaluator;
import org.neo4j.graphdb.StopEvaluator;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.TraversalPosition;
import org.neo4j.graphdb.Traverser;
import org.neo4j.kernel.StandardExpander;
import org.neo4j.kernel.Traversal;
import org.neo4j.kernel.Uniqueness;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.TripleMatch;
import com.hp.hpl.jena.graph.impl.GraphBase;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.util.iterator.Filter;
import com.hp.hpl.jena.util.iterator.NullIterator;
import com.hp.hpl.jena.util.iterator.Map1;
import com.hp.hpl.jena.util.iterator.Map1Iterator;
import com.hp.hpl.jena.util.iterator.NiceIterator;
import com.hp.hpl.jena.query.QueryExecException;

import com.hp.hpl.jena.vocabulary.RDF;

import com.turbulence.core.ClusterSpace;

import com.turbulence.util.AllColumnsIterator;
import com.turbulence.util.ConceptsInstancesIterator;
import com.turbulence.util.ObjectsFilterKeepIterator;

import me.prettyprint.cassandra.serializers.StringSerializer;

import me.prettyprint.cassandra.service.template.SuperCfResult;

import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HSuperColumn;

import me.prettyprint.hector.api.factory.HFactory;

import me.prettyprint.hector.api.query.SliceQuery;

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
        Index<org.neo4j.graphdb.Node> conceptIndex = cs.index().forNodes("conceptIndex");
        return conceptIndex.get("CONCEPT", classIRI).getSingle();
    }

    private org.neo4j.graphdb.Node getObjectProperty(final String objectPropertyIRI) {
        String ontologyIRI;
        if (objectPropertyIRI.lastIndexOf('#') != -1) {
            ontologyIRI = objectPropertyIRI.substring(0, objectPropertyIRI.lastIndexOf('#'));
        }
        else {
            ontologyIRI = objectPropertyIRI.substring(0, objectPropertyIRI.lastIndexOf('/')+1);
        }
        Index<org.neo4j.graphdb.Node> ontologyIndex = cs.index().forNodes("ontologyIndex");
        org.neo4j.graphdb.Node ont = ontologyIndex.get("KNOWN_ONTOLOGY", ontologyIRI).getSingle();
        if (ont == null)
            return null;
        // get the class
        Traverser trav = ont.traverse(Traverser.Order.BREADTH_FIRST, StopEvaluator.DEPTH_ONE, new ReturnableEvaluator() {
            public boolean isReturnableNode(TraversalPosition pos) {
                return ((String)pos.currentNode().getProperty("IRI")).equals(objectPropertyIRI.toString());
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
    private Iterable<org.neo4j.graphdb.Node> classCover(final String classIRI) {
        org.neo4j.graphdb.Node cNode = getClass(classIRI);
        if (cNode == null)
            return new ArrayList<org.neo4j.graphdb.Node>();

        TraversalDescription desc = Traversal.description()
                                    .depthFirst()
                                    .evaluator(Evaluators.all())
                                    .relationships(ClusterSpace.PublicRelTypes.IS_A, Direction.INCOMING)
                                    .relationships(ClusterSpace.PublicRelTypes.EQUIVALENT_CLASS, Direction.BOTH);

        //return new ClusterSpaceJenaIterator(desc.traverse(cNode).relationships().iterator());
        return desc.traverse(cNode).nodes();
    }

    // TODO don't put equivalent classes here
    private Iterable<org.neo4j.graphdb.Node> superclasses(final org.neo4j.graphdb.Node cNode) {
        TraversalDescription desc = Traversal.description()
                                    .depthFirst()
                                    .evaluator(Evaluators.all())
                                    .relationships(ClusterSpace.PublicRelTypes.IS_A, Direction.OUTGOING)
                                    .relationships(ClusterSpace.PublicRelTypes.EQUIVALENT_CLASS, Direction.BOTH);

        return desc.traverse(cNode).nodes();
    }

    private NiceIterator<String> objectPropertyCover(final String baseObjectPropertyIRI) {
        org.neo4j.graphdb.Node baseNode = getClass(baseObjectPropertyIRI);
        if (baseNode == null)
            return NullIterator.instance();

        TraversalDescription desc = Traversal.description()
                                    .depthFirst()
                                    .evaluator(Evaluators.all())
                                    .relationships(ClusterSpace.PublicRelTypes.IS_A, Direction.INCOMING)
                                    .relationships(ClusterSpace.PublicRelTypes.EQUIVALENT_OBJECT_PROPERTY, Direction.BOTH);

        return new Map1Iterator<org.neo4j.graphdb.Node, String>(
            new Map1<org.neo4j.graphdb.Node, String>() {
                public String map1(org.neo4j.graphdb.Node node) {
                    return (String) node.getProperty("IRI");
                }
            },
            desc.traverse(baseNode).nodes().iterator()
        );
    }

    protected ExtendedIterator<Triple> graphBaseFind(TripleMatch tm) {
        Triple triple = tm.asTriple();
        Node sub  = triple.getSubject();
        Node pred = triple.getPredicate();
        Node obj  = triple.getObject();
        //logger.warn("graphBaseFind: " + sub + " -- " + pred + " -- " + obj);

        TraversalDescription trav = Traversal.description()
                                    .breadthFirst()
                                    .uniqueness(Uniqueness.NONE)
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

        /*if (sub.isURI()) {
            startNode = getClass(sub.getURI());
            if (startNode == null)
                return new ClusterSpaceJenaIterator(EmptyIterator.INSTANCE);

            relationshipDirection = Direction.OUTGOING;

            trav = trav.evaluator(Evaluators.atDepth(1));

            if (obj.isURI()) {
                org.neo4j.graphdb.Node objNode = getClass(obj.getURI());
                if (objNode == null)
                    return new ClusterSpaceJenaIterator(EmptyIterator.INSTANCE);

//                trav = trav.evaluator(Evaluators.returnWhereEndNodeIs(objNode));
//                for (org.neo4j.graphdb.Node equiv : cs.getEquivalentClasses(objNode))
//                    trav = trav.evaluator(Evaluators.returnWhereEndNodeIs(equiv));
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
        }*/

        if (pred.isURI()
            && pred.getURI().equals(RDF.type.getURI())) {
            if (!obj.isURI())
                throw new QueryExecException("'a' predicate expects URI object");

            ExtendedIterator<Triple> total = NullIterator.instance();
            for (org.neo4j.graphdb.Node n : classCover(obj.getURI())) {
                String classIRI = (String)n.getProperty("IRI");

                SliceQuery<String, String, String> query = HFactory.createSliceQuery(TurbulenceDriver.getKeyspace(), StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
                query.setKey(classIRI);
                query.setColumnFamily("Concepts");

                Iterator<HColumn<String, String>> it = new AllColumnsIterator<String, String>(query);
                total = total.andThen(new ConceptsInstancesIterator(classIRI, it));
            }
            return total;
        }
        /*else if (pred.isURI()
            && pred.getURI().equals("http://www.w3.org/2000/01/rdf-schema#subClassOf")) {
            trav = trav.relationships(ClusterSpace.PublicRelTypes.IS_A, relationshipDirection);
            trav = trav.relationships(ClusterSpace.PublicRelTypes.EQUIVALENT_CLASS); // equivalent class never has direction
        }*/
        else if (pred.isURI()) { /* custom relationship */
            return handleCustomRelationship(sub, pred, obj);
        }
        else if (pred.equals(Node.ANY)) {
            if (sub.isURI() && obj.isURI()) {
            }
            else {
                throw new RuntimeException("Arbitrary relationship expects well defined subject and object");
            }

            org.neo4j.graphdb.Node subNode = getClass(sub.getURI());
            org.neo4j.graphdb.Node objNode = getClass(obj.getURI());

            if (subNode == null || objNode == null) {
                throw new RuntimeException("Unknown concepts");
            }

            StandardExpander expander = StandardExpander.DEFAULT
                                        .add(ClusterSpace.PublicRelTypes.EQUIVALENT_CLASS)
                                        .add(ClusterSpace.PublicRelTypes.IS_A)
                                        .add(ClusterSpace.PublicRelTypes.OBJECT_RELATIONSHIP);

            PathFinder<Path> pf = GraphAlgoFactory.allSimplePaths(expander, 4);

            ExtendedIterator<Triple> it = new ClusterSpaceJenaIterator(EmptyIterator.INSTANCE);
            for (Path path : pf.findAllPaths(subNode, objNode)) {
                it = it.andThen(new ClusterSpaceJenaIterator(path.relationships().iterator()));
            }
            return it;
        }
        else {
            throw new QueryExecException("Unknown relationship type");
        }

        //return new ClusterSpaceJenaIterator(trav.traverse(startNode).relationships().iterator());
    }

    public ExtendedIterator<Triple> handleCustomRelationship(Node sub, Node pred, Node obj) {
        org.neo4j.graphdb.Node subjectClass = null;
        org.neo4j.graphdb.Node objectClass = null;

        if (sub.equals(Node.ANY)) {
            if (obj.equals(Node.ANY)) {
                throw new UnsupportedOperationException();
            }
            else if (obj.isURI() && (objectClass = getClass(obj.getURI())) != null) {
                // TODO handle entire cover in types check
                return new ClusterSpaceJenaIterator(EmptyIterator.INSTANCE);
            }
            else if (obj.isURI()) { // could be an instance
                Filter<HColumn<String, String>> filter = new Filter<HColumn<String, String>>() {
                    public boolean accept(HColumn<String, String> o) {
                        return true;
                    }
                };

                // since we need to invert the OPS triple to SPO
                Map1<Triple, Triple> tripleSwap = new Map1<Triple, Triple>() {
                    public Triple map1(Triple from) {
                        return Triple.create(from.getObject(), from.getPredicate(), from.getSubject());
                    }
                };
                ObjectsFilterKeepIterator it = new ObjectsFilterKeepIterator(obj.getURI(), pred.getURI(), filter, "OPSData");
                return new Map1Iterator(tripleSwap, it);
            }
            else if (obj.isLiteral()) {
                return new ClusterSpaceJenaIterator(EmptyIterator.INSTANCE);
            }
            else {
                throw new QueryExecException("Object is of unknown type");
            }
        }
        else if (sub.isURI() && (subjectClass = getClass(sub.getURI())) != null) {
                // TODO handle entire cover in types check
            SliceQuery<String, String, String> query = HFactory.createSliceQuery(TurbulenceDriver.getKeyspace(), StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
            query.setColumnFamily("Concepts");
            query.setKey(sub.getURI());
            Iterator<HColumn<String, String>> instances = new AllColumnsIterator(query);

            if (obj.equals(Node.ANY)) {
                return new ClusterSpaceJenaIterator(EmptyIterator.INSTANCE);
            }
            else if (obj.isURI() && (objectClass = getClass(obj.getURI())) != null) {
                // TODO handle entire cover in types check
                // TODO abstract away and make iterable
                ExtendedIterator<Triple> it = new NiceIterator<Triple>();
                final String objectTypeURI = obj.getURI();
                Filter<HColumn<String, String>> filter = new Filter<HColumn<String, String>>() {
                    public boolean accept(HColumn<String, String> o) {
                        // check if o.type is objectTypeURI
                        HColumn<String, String> type = TurbulenceDriver.getSPODataTemplate().querySingleSubColumn(o.getValue(), RDF.type.getURI(), "URI|" + DigestUtils.md5Hex(objectTypeURI), StringSerializer.get());
                        return type != null ? type.getValue().equals(objectTypeURI) : false;
                    }
                };

                while (instances.hasNext()) {
                    HColumn<String, String> instance = instances.next();
                    it = it.andThen(new ObjectsFilterKeepIterator(instance.getValue(), pred.getURI(), filter, "SPOData"));
                }
                return it;
            }
            else if (obj.isURI()) {
                return new ClusterSpaceJenaIterator(EmptyIterator.INSTANCE);
            }
            else if (obj.isLiteral()) {
                return new ClusterSpaceJenaIterator(EmptyIterator.INSTANCE);
            }
            else {
                throw new QueryExecException("Object is of unknown type");
            }
        }
        else if (sub.isURI()) {
            Filter<HColumn<String, String>> filter = null;
            if (obj.equals(Node.ANY)) {
                // CHECKED 31/3/12 9:59
                filter = new Filter<HColumn<String, String>>() {
                    public boolean accept(HColumn<String, String> o) {
                        return true;
                    }
                };
            }
            else if (obj.isURI() && (objectClass = getClass(obj.getURI())) != null) {
                // TODO handle entire cover in types check
                filter = new Filter<HColumn<String, String>>() {
                    public boolean accept(HColumn<String, String> o) {
                        return true;
                    }
                };
            }
            else if (obj.isURI()) {
                // CHECKED 31/3/12 10:02
                final String objectURI = obj.getURI();
                filter = new Filter<HColumn<String, String>>() {
                    public boolean accept(HColumn<String, String> o) {
                        return o.getValue().equals(objectURI);
                    }
                };
            }
            else if (obj.isLiteral()) {
                final String objectValue = obj.getLiteral().toString();
                filter = new Filter<HColumn<String, String>>() {
                    public boolean accept(HColumn<String, String> o) {
                        return o.getValue().equals(objectValue);
                    }
                };
            }
            else {
                throw new QueryExecException("Object is of unknown type");
            }
            ObjectsFilterKeepIterator iterator = new ObjectsFilterKeepIterator(sub.getURI(), pred.getURI(), filter, "SPOData");
            return iterator;
        }
        else {
            throw new QueryExecException("Subject is of unknown type");
        }
        /*// if object is ANY, return concepts that have a relationship to*/
        //// the subject
        //if (obj.equals(Node.ANY)) {
        //}
        //else {
        //// if it is a concept, return all the instances for *all* the
        //// cover of subjects which have a ObjectProperty to obj
        //if (obj.isURI() && (Node objClass = getClass(obj.getURI()))) {
        //}
        //else if (obj.isURI()) { // it is a instance, return all subjects which have the relationship pred to object instance obj
        //}
        //else if (obj.isLiteral()) { // it is a literal, do a data property comparison, all subjects which have the relationship and the object instance matches
        //}
        //}
        //final Set<String> iris = IteratorCollection.iteratorToSet(objectPropertyCover(pred.getURI()));
        //trav = trav.relationships(ClusterSpace.PublicRelTypes.OBJECT_RELATIONSHIP, relationshipDirection);
        //trav = trav.evaluator(new Evaluator() {
        //public Evaluation evaluate(Path path) {
        //if (path.lastRelationship() != null
        //&& path.lastRelationship().getType().equals(ClusterSpace.PublicRelTypes.OBJECT_RELATIONSHIP)
        //&& iris.contains((String)path.lastRelationship().getProperty("IRI"))) {
        //return Evaluation.INCLUDE_AND_PRUNE;
        //}

        //return Evaluation.EXCLUDE_AND_CONTINUE;
        //}
        //});

        //ExtendedIterator<Triple> result = new ClusterSpaceJenaIterator(trav.traverse(startNode).relationships().iterator());
        //final Iterable<org.neo4j.graphdb.Node> superClasses = superclasses(startNode);
        //// try all superclasses also since the relationship
        //// domain might actually be a superclass
        //for (org.neo4j.graphdb.Node n : superClasses) {
        //result = result.andThen(new ClusterSpaceJenaIterator(trav.traverse(n).relationships().iterator()));
        //}
        /*return result;*/
    }
}
