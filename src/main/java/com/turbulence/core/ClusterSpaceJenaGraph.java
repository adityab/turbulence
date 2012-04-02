package com.turbulence.core;

import java.net.URI;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

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
import org.neo4j.graphdb.traversal.Evaluator;
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
import com.hp.hpl.jena.util.iterator.IteratorIterator;
import com.hp.hpl.jena.util.iterator.NullIterator;
import com.hp.hpl.jena.util.iterator.Map1;
import com.hp.hpl.jena.util.iterator.Map1Iterator;
import com.hp.hpl.jena.util.iterator.NiceIterator;
import com.hp.hpl.jena.query.QueryExecException;

import com.hp.hpl.jena.util.iterator.WrappedIterator;

import com.hp.hpl.jena.util.IteratorCollection;

import com.hp.hpl.jena.vocabulary.RDF;

import com.turbulence.core.ClusterSpace;

import com.turbulence.util.AllColumnsIterator;
import com.turbulence.util.AllSubColumnsIterator;
import com.turbulence.util.AllSuperColumnsIterator;
import com.turbulence.util.ConceptsInstancesIterator;
import com.turbulence.util.InstancesFilterKeepIterator;

import me.prettyprint.cassandra.serializers.StringSerializer;

import me.prettyprint.cassandra.service.template.SuperCfResult;

import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HSuperColumn;

import me.prettyprint.hector.api.factory.HFactory;

import me.prettyprint.hector.api.query.SliceQuery;
import me.prettyprint.hector.api.query.SubSliceQuery;
import me.prettyprint.hector.api.query.SuperSliceQuery;

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
        final Node sub  = triple.getSubject();
        final Node pred = triple.getPredicate();
        final Node obj  = triple.getObject();

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
        else if (pred.isURI()) { /* custom relationship */
            return handleCustomRelationship(sub, pred, obj);
        }
        else if (pred.equals(Node.ANY)) {
            if (sub.equals(Node.ANY) && obj.equals(Node.ANY)) {
                throw new QueryExecException("predicate ANY cannot have both subject and object as ANY");
            }
            else if (sub.isURI() && getClass(sub.getURI()) != null) {
                // TODO each call to handleCustomRelationshipSubjectConcept
                // recomputes the set of instances!
                // TODO handle relationship cover
                TraversalDescription relationships = Traversal.description().breadthFirst().uniqueness(Uniqueness.NONE).evaluator(Evaluators.atDepth(1)).relationships(ClusterSpace.PublicRelTypes.OBJECT_RELATIONSHIP, Direction.OUTGOING);

                // class cover is handled by
                // handleCustomRelationshipSubjectConcept
                Map1<Relationship, Iterator<Triple>> map = new Map1<Relationship, Iterator<Triple>>() {
                    public Iterator<Triple> map1(Relationship r) {
                        return handleCustomRelationshipSubjectConcept(sub, Node.createURI((String)r.getProperty("IRI")), obj);
                    }
                };

                return WrappedIterator.create(new IteratorIterator<Triple>(new Map1Iterator<Relationship, Iterator<Triple>>(map, relationships.traverse(getClass(sub.getURI())).relationships().iterator())));
            }
            else if (sub.isURI()) {
                SuperSliceQuery<String, String, String, String> predQuery = HFactory.createSuperSliceQuery(TurbulenceDriver.getKeyspace(), StringSerializer.get(), StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
                predQuery.setKey(sub.getURI());
                predQuery.setColumnFamily("SPOData");
                Map1<HSuperColumn<String, String, String>, Iterator<Triple>> predicateObjectsMap = new Map1<HSuperColumn<String, String, String>, Iterator<Triple>>() {
                    public Iterator<Triple> map1(HSuperColumn<String, String, String> sc) {
                        String predicate = sc.getName();
                        return handleCustomRelationshipSubjectInstance(sub, Node.createURI(predicate), obj);
                    }
                };
                return WrappedIterator.create(new IteratorIterator<Triple>(new Map1Iterator<HSuperColumn<String, String, String>, Iterator<Triple>>(predicateObjectsMap, new AllSuperColumnsIterator(predQuery))));
            }
            else if (sub.equals(Node.ANY)) {
                if (obj.isURI() && getClass(obj.getURI()) != null) {
                    // TODO does not work
                    // TODO each call to handleCustomRelationshipSubjectAny
                    // recomputes the set of instances!
                    // TODO handle relationship cover
                    TraversalDescription relationships = Traversal.description().breadthFirst().uniqueness(Uniqueness.NONE).evaluator(Evaluators.atDepth(1)).relationships(ClusterSpace.PublicRelTypes.OBJECT_RELATIONSHIP, Direction.INCOMING);

                    // class cover is handled by
                    // handleCustomRelationshipSubjectConcept
                    Map1<Relationship, Iterator<Triple>> map = new Map1<Relationship, Iterator<Triple>>() {
                        public Iterator<Triple> map1(Relationship r) {
                            return handleCustomRelationshipSubjectAny(sub, Node.createURI((String)r.getProperty("IRI")), obj);
                        }
                    };

                    return WrappedIterator.create(new IteratorIterator<Triple>(new Map1Iterator<Relationship, Iterator<Triple>>(map, relationships.traverse(getClass(obj.getURI())).relationships().iterator())));
                }
                else if (obj.isURI()) {
                    SuperSliceQuery<String, String, String, String> predQuery = HFactory.createSuperSliceQuery(TurbulenceDriver.getKeyspace(), StringSerializer.get(), StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
                    predQuery.setKey(obj.getURI());
                    predQuery.setColumnFamily("OPSData");

                    final Map1<Triple, Triple> tripleSwap = new Map1<Triple, Triple>() {
                        public Triple map1(Triple from) {
                            return Triple.create(from.getObject(), from.getPredicate(), from.getSubject());
                        }
                    };

                    Map1<HSuperColumn<String, String, String>, Iterator<Triple>> predicateObjectsMap = new Map1<HSuperColumn<String, String, String>, Iterator<Triple>>() {
                        public Iterator<Triple> map1(HSuperColumn<String, String, String> sc) {
                            String predicate = sc.getName();
                            Filter<HColumn<String, String>> filter = Filter.any();
                            return new Map1Iterator<Triple, Triple>(tripleSwap, new InstancesFilterKeepIterator(obj.getURI(), predicate, filter, "OPSData"));
                        }
                    };
                    return WrappedIterator.create(new IteratorIterator<Triple>(new Map1Iterator<HSuperColumn<String, String, String>, Iterator<Triple>>(predicateObjectsMap, new AllSuperColumnsIterator(predQuery))));
                }
                else if (obj.isLiteral()) {
                    throw new UnsupportedOperationException();
                }
                throw new QueryExecException("Something went very wrong when sub equals Node.ANY");
            }
            throw new QueryExecException("Something went very wrong");
        }
        else {
            throw new QueryExecException("Unknown relationship type");
        }

    }

    private org.neo4j.graphdb.Node isObjectPropertyRange(final String objectPropertyIRI, final org.neo4j.graphdb.Node rangeClass) {
        // look in the superclasses for existence of this relationship
        TraversalDescription trav = Traversal.description().breadthFirst().uniqueness(Uniqueness.NONE).evaluator(Evaluators.atDepth(1)).evaluator(Evaluators.excludeStartPosition()).relationships(ClusterSpace.PublicRelTypes.OBJECT_RELATIONSHIP, Direction.INCOMING).evaluator(new Evaluator() {
            public Evaluation evaluate(Path path) {
                if (path.lastRelationship() != null
                    && ((String)path.lastRelationship().getProperty("IRI")).equals(objectPropertyIRI)) {
                    return Evaluation.INCLUDE_AND_CONTINUE;
                }
                return Evaluation.EXCLUDE_AND_CONTINUE;
            }
        });

        for (org.neo4j.graphdb.Node n : superclasses(rangeClass)) {
            Iterator<org.neo4j.graphdb.Node> it = trav.traverse(n).nodes().iterator();
            if (it.hasNext())
                return it.next();
        }
        return null;
    }

    public ExtendedIterator<Triple> handleCustomRelationship(final Node sub, final Node pred, final Node obj) {
        org.neo4j.graphdb.Node subjectClass = null;

        if (sub.equals(Node.ANY)) {
            return handleCustomRelationshipSubjectAny(sub, pred, obj);
        }
        else if (sub.isURI() && (subjectClass = getClass(sub.getURI())) != null) {
            return handleCustomRelationshipSubjectConcept(sub, pred, obj);
        }
        else if (sub.isURI()) {
            return handleCustomRelationshipSubjectInstance(sub, pred, obj);
        }
        else {
            throw new QueryExecException("Subject is of unknown type");
        }
    }

    private ExtendedIterator<Triple> handleCustomRelationshipSubjectAny(final Node sub, final Node pred, final Node obj) {
        org.neo4j.graphdb.Node objectClass = null;
        if (obj.equals(Node.ANY)) {
            throw new UnsupportedOperationException();
        }
        else if (obj.isURI() && (objectClass = getClass(obj.getURI())) != null) {
            // TODO handle equivalent object properties coming in also
            final String objectPropertyIRI = pred.getURI();
            org.neo4j.graphdb.Node domain = isObjectPropertyRange(objectPropertyIRI, objectClass);
            if (domain == null)
                return new NullIterator<Triple>();
            logger.warn("Domain " + domain.getProperty("IRI"));

            // for each class in cover of domain
                // for every instance of the class
                    // if instance -> pred -> o  and o.type in object cover
                        // emit the instance -> pred -> o pair
            final Set<String> rangeCoverIRIs = IteratorCollection.iteratorToSet(new Map1Iterator<org.neo4j.graphdb.Node, String>(new Map1<org.neo4j.graphdb.Node, String>() {
                public String map1(org.neo4j.graphdb.Node from) {
                    return (String) from.getProperty("IRI");
                }
            }, classCover(obj.getURI()).iterator()));
            logger.warn("Range cover " + rangeCoverIRIs);

            Filter<HColumn<String, String>> filter = new Filter<HColumn<String, String>>() {
                public boolean accept(HColumn<String, String> o) {
                    SubSliceQuery<String, String, String, String> query
                        = HFactory.createSubSliceQuery(TurbulenceDriver.getKeyspace(),
                                StringSerializer.get(), StringSerializer.get(),
                                StringSerializer.get(), StringSerializer.get());
                    query.setKey(o.getValue());
                    query.setSuperColumn(RDF.type.getURI());
                    query.setColumnFamily("SPOData");
                    AllSubColumnsIterator<String, String> it = new AllSubColumnsIterator<String, String>(query);
                    while (it.hasNext()) {
                        HColumn<String, String> col = it.next();
                        if (rangeCoverIRIs.contains(col.getValue())) {
                            return true;
                        }
                    }
                    return false;
                }
            };


            ExtendedIterator<Triple> result = NullIterator.instance();
            for (org.neo4j.graphdb.Node domainNode : classCover((String)domain.getProperty("IRI"))) {
                SliceQuery<String, String, String> query = HFactory.createSliceQuery(TurbulenceDriver.getKeyspace(), StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
                query.setColumnFamily("Concepts");
                query.setKey((String)domainNode.getProperty("IRI"));
                Iterator<HColumn<String, String>> instances = new AllColumnsIterator<String, String>(query);

                while (instances.hasNext()) {
                    String instance = instances.next().getValue();
                    InstancesFilterKeepIterator objects = new InstancesFilterKeepIterator(instance, pred.getURI(), filter, "SPOData");
                    result = result.andThen(objects);
                }
            }
            return result;
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
            InstancesFilterKeepIterator it = new InstancesFilterKeepIterator(obj.getURI(), pred.getURI(), filter, "OPSData");
            return new Map1Iterator(tripleSwap, it);
        }
        else if (obj.isLiteral()) {
            return new ClusterSpaceJenaIterator(EmptyIterator.INSTANCE);
        }
        else {
            throw new QueryExecException("Object is of unknown type");
        }
    }

    private ExtendedIterator<Triple> handleCustomRelationshipSubjectConcept(final Node sub, final Node pred, final Node obj) {
        org.neo4j.graphdb.Node objectClass = null;
        ExtendedIterator<HColumn<String, String>> instances = NullIterator.instance();

        for (org.neo4j.graphdb.Node n : classCover(sub.getURI())) {
            SliceQuery<String, String, String> query = HFactory.createSliceQuery(TurbulenceDriver.getKeyspace(), StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
            query.setColumnFamily("Concepts");
            query.setKey((String)n.getProperty("IRI"));
            instances = instances.andThen(new AllColumnsIterator(query));
        }

        if (obj.equals(Node.ANY)) {
            final String predicateURI = pred.getURI();
            final Filter<HColumn<String, String>> anyFilter = Filter.any();
            Map1<HColumn<String, String>, Iterator<Triple>> map = new Map1<HColumn<String, String>, Iterator<Triple>>() {
                public Iterator<Triple> map1(HColumn<String, String> column) {
                    return new InstancesFilterKeepIterator(column.getValue(), predicateURI, anyFilter, "SPOData");
                }
            };
            Map1Iterator<HColumn<String, String>, Iterator<Triple>> instanceTriples = new Map1Iterator<HColumn<String, String>, Iterator<Triple>>(map, instances);

            return WrappedIterator.create(new IteratorIterator<Triple>(instanceTriples));
        }
        else if (obj.isURI() && (objectClass = getClass(obj.getURI())) != null) {
            // TODO handle equivalent object properties coming in also
            final String objectPropertyIRI = pred.getURI();
            org.neo4j.graphdb.Node domain = isObjectPropertyRange(objectPropertyIRI, objectClass);
            if (domain == null)
                return new NullIterator<Triple>();

            // for each class in cover of domain
                // for every instance of the class
                    // if instance -> pred -> o  and o.type in object cover
                        // emit the instance -> pred -> o pair
            final Set<String> rangeCoverIRIs = IteratorCollection.iteratorToSet(new Map1Iterator<org.neo4j.graphdb.Node, String>(new Map1<org.neo4j.graphdb.Node, String>() {
                public String map1(org.neo4j.graphdb.Node from) {
                    return (String) from.getProperty("IRI");
                }
            }, classCover(obj.getURI()).iterator()));

            final Filter<HColumn<String, String>> filter = new Filter<HColumn<String, String>>() {
                public boolean accept(HColumn<String, String> o) {
                    SubSliceQuery<String, String, String, String> query
                        = HFactory.createSubSliceQuery(TurbulenceDriver.getKeyspace(),
                                StringSerializer.get(), StringSerializer.get(),
                                StringSerializer.get(), StringSerializer.get());
                    query.setKey(o.getValue());
                    query.setSuperColumn(RDF.type.getURI());
                    query.setColumnFamily("SPOData");
                    AllSubColumnsIterator<String, String> it = new AllSubColumnsIterator<String, String>(query);
                    while (it.hasNext()) {
                        HColumn<String, String> col = it.next();
                        if (rangeCoverIRIs.contains(col.getValue())) {
                            return true;
                        }
                    }
                    return false;
                }
            };

            final String predicateURI = pred.getURI();
            Map1<HColumn<String, String>, Iterator<Triple>> map = new Map1<HColumn<String, String>, Iterator<Triple>>() {
                public Iterator<Triple> map1(HColumn<String, String> column) {
                    return new InstancesFilterKeepIterator(column.getValue(), predicateURI, filter, "SPOData");
                }
            };
            Map1Iterator<HColumn<String, String>, Iterator<Triple>> instanceTriples = new Map1Iterator<HColumn<String, String>, Iterator<Triple>>(map, instances);

            return WrappedIterator.create(new IteratorIterator<Triple>(instanceTriples));
        }
        else if (obj.isURI()) {
            final Set<String> domainIRIs = IteratorCollection.iteratorToSet(new Map1Iterator<org.neo4j.graphdb.Node, String>(new Map1<org.neo4j.graphdb.Node, String>() {
                public String map1(org.neo4j.graphdb.Node from) {
                    return (String) from.getProperty("IRI");
                }
            }, classCover(sub.getURI()).iterator()));

            final Filter<HColumn<String, String>> filter = new Filter<HColumn<String, String>>() {
                public boolean accept(HColumn<String, String> o) {
                    SubSliceQuery<String, String, String, String> query
                        = HFactory.createSubSliceQuery(TurbulenceDriver.getKeyspace(),
                                StringSerializer.get(), StringSerializer.get(),
                                StringSerializer.get(), StringSerializer.get());
                    query.setKey(o.getValue());
                    query.setSuperColumn(RDF.type.getURI());
                    query.setColumnFamily("SPOData");
                    AllSubColumnsIterator<String, String> it = new AllSubColumnsIterator<String, String>(query);
                    while (it.hasNext()) {
                        HColumn<String, String> col = it.next();
                        if (domainIRIs.contains(col.getValue())) {
                            return true;
                        }
                    }
                    return false;
                }
            };

            // since we need to invert the OPS triple to SPO
            Map1<Triple, Triple> tripleSwap = new Map1<Triple, Triple>() {
                public Triple map1(Triple from) {
                    return Triple.create(from.getObject(), from.getPredicate(), from.getSubject());
                }
            };
            InstancesFilterKeepIterator it = new InstancesFilterKeepIterator(obj.getURI(), pred.getURI(), filter, "OPSData");
            return new Map1Iterator(tripleSwap, it);
        }
        else if (obj.isLiteral()) {
            final String predicateURI = pred.getURI();
            final String objectValue = obj.getLiteral().toString();
            final Filter<HColumn<String, String>> filter = new Filter<HColumn<String, String>>() {
                public boolean accept(HColumn<String, String> o) {
                    return o.getValue().equals(objectValue);
                }
            };
            Map1<HColumn<String, String>, Iterator<Triple>> map = new Map1<HColumn<String, String>, Iterator<Triple>>() {
                public Iterator<Triple> map1(HColumn<String, String> column) {
                    return new InstancesFilterKeepIterator(column.getValue(), predicateURI, filter, "SPOData");
                }
            };
            Map1Iterator<HColumn<String, String>, Iterator<Triple>> instanceTriples = new Map1Iterator<HColumn<String, String>, Iterator<Triple>>(map, instances);

            return WrappedIterator.create(new IteratorIterator<Triple>(instanceTriples));
        }
        else {
            throw new QueryExecException("Object is of unknown type");
        }
    }

    private ExtendedIterator<Triple> handleCustomRelationshipSubjectInstance(final Node sub, final Node pred, final Node obj) {
        org.neo4j.graphdb.Node objectClass = null;
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
        InstancesFilterKeepIterator iterator = new InstancesFilterKeepIterator(sub.getURI(), pred.getURI(), filter, "SPOData");
        return iterator;
    }
}
