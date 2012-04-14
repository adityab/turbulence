package com.turbulence.core.actions;

import java.io.File;
import java.io.InputStream;
import java.io.IOException;

import java.net.URI;
import java.net.URISyntaxException;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.IteratorUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.jdom2.Attribute;
import org.jdom2.Document;
import org.jdom2.Element;

import org.jdom2.input.sax.XMLReaders;

import org.jdom2.input.SAXBuilder;

import org.jdom2.JDOMException;

import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Expander;

import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;

import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;

import org.neo4j.helpers.Predicate;

import org.neo4j.index.lucene.QueryContext;

import org.neo4j.kernel.StandardExpander;
import org.neo4j.kernel.Traversal;
import org.neo4j.kernel.Uniqueness;

import org.xml.sax.SAXException;

import com.hp.hpl.jena.graph.Triple;

import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;

import com.hp.hpl.jena.util.iterator.Filter;

import com.hp.hpl.jena.util.IteratorCollection;

import com.hp.hpl.jena.vocabulary.RDF;

import com.turbulence.core.ClusterSpace;
import com.turbulence.core.TurbulenceDriver;

class DatatypePropertyMatcher {
    private static final Log logger =
        LogFactory.getLog(DatatypePropertyMatcher.class);
    private Set<Relationship> properties;

    DatatypePropertyMatcher(Node clazz) {
        TraversalDescription trav = Traversal.description()
            .breadthFirst()
            .uniqueness(Uniqueness.RELATIONSHIP_GLOBAL)
            .evaluator(Evaluators.returnWhereLastRelationshipTypeIs(ClusterSpace.PublicRelTypes.DATATYPE_RELATIONSHIP))
            .relationships(ClusterSpace.PublicRelTypes.DATATYPE_RELATIONSHIP)
            .relationships(ClusterSpace.PublicRelTypes.EQUIVALENT_CLASS)
            .relationships(ClusterSpace.PublicRelTypes.IS_A, Direction.OUTGOING);

        properties = IteratorCollection.iteratorToSet(trav.traverse(clazz).relationships().iterator());
    }

    public Relationship match(final String possibleName) {

        Filter<Relationship> filter = new Filter<Relationship>() {
            public boolean accept(Relationship o) {
                String iri = (String) o.getProperty("IRI");
                String base = null;
                try {
                    URI uri = new URI(iri);
                    if (uri.getFragment() != null)
                        base = uri.getFragment();
                    else
                        base = new File(uri.getPath()).getName();
                } catch (URISyntaxException e) {
                    return false;
                }

                //logger.warn(possibleCanon + " " + base);
                // conditions
                if (base.equals(possibleName))
                    return true;

                return false;
            }
        };

        List<Relationship> matches = IteratorCollection.iteratorToList(filter.filterKeep(properties.iterator()));
        if (matches.size() > 0)
            return matches.get(0);

        return null;
    }
}

class ObjectPropertyMatcher {
    private static final Log logger =
        LogFactory.getLog(ObjectPropertyMatcher.class);
    private Set<Relationship> properties;

    ObjectPropertyMatcher(Node clazz) {
        TraversalDescription trav = Traversal.description()
            .breadthFirst()
            .uniqueness(Uniqueness.RELATIONSHIP_GLOBAL)
            .evaluator(Evaluators.returnWhereLastRelationshipTypeIs(ClusterSpace.PublicRelTypes.OBJECT_RELATIONSHIP))
            .relationships(ClusterSpace.PublicRelTypes.OBJECT_RELATIONSHIP)
            .relationships(ClusterSpace.PublicRelTypes.EQUIVALENT_CLASS)
            .relationships(ClusterSpace.PublicRelTypes.IS_A, Direction.OUTGOING);

        properties = IteratorCollection.iteratorToSet(trav.traverse(clazz).relationships().iterator());
    }

    public Relationship match(final String possibleName) {
        Filter<Relationship> filter = new Filter<Relationship>() {
            public boolean accept(Relationship o) {
                String iri = (String) o.getProperty("IRI");
                String base = null;
                try {
                    URI uri = new URI(iri);
                    if (uri.getFragment() != null)
                        base = uri.getFragment();
                    else
                        base = new File(uri.getPath()).getName();
                } catch (URISyntaxException e) {
                    return false;
                }

                if (base.equals(possibleName))
                    return true;

                return false;
            }
        };

        List<Relationship> matches = IteratorCollection.iteratorToList(filter.filterKeep(properties.iterator()));
        if (matches.size() > 0)
            return matches.get(0);

        return null;
    }
}

public class StoreXMLDataAction implements Action {
    private static final Log logger =
        LogFactory.getLog(StoreXMLDataAction.class);


    private InputStream input;
    private Index<Node> conceptIndex;
    private Set<Triple> triples;
    private Model model;

    protected StoreXMLDataAction(InputStream in) {
        input = in;
    }

    private boolean matchApproximate(String needle, Set<Node> haystack) {
        return false;
    }

    private RDFNode toRDF(Element element) {
        // try to find concept
        IndexHits<Node> hits = conceptIndex.query("CONCEPT", new QueryContext("*" + element.getName()).sortByScore());

        /*Map<String, Set<Node>> msn = new HashMap<String, Set<Node>>();
        for (Node n : hits) {
            for (String c : getClassCover(n))
        }*/
        // try to match based on number of properties matching
        // element children and compared with object/datatype properties
        // and consider superclasses also

        // for now only deal with the first hit
        Node n = hits.hasNext() ? hits.next() : null;

        if (n == null)
            return null;

        Resource subject = model.createResource();

        // rdf:type
        model.add(model.createStatement(subject, RDF.type, model.createResource((String)n.getProperty("IRI"))));

        DatatypePropertyMatcher dpMatcher = new DatatypePropertyMatcher(n);
        ObjectPropertyMatcher opMatcher = new ObjectPropertyMatcher(n);
        for (Attribute attr : element.getAttributes()) {
            Relationship rel = dpMatcher.match(attr.getName());
            if (rel == null)
                continue;
            model.add(model.createLiteralStatement(subject, model.createProperty((String)rel.getProperty("IRI")), attr.getValue()));
        }

        // TODO: possible that no children
        for (Element child : element.getChildren()) {
            // a possible data/object property
            if (child.getChildren().isEmpty()) {
                Relationship dpRel = dpMatcher.match(child.getName());

                Relationship opRel = opMatcher.match(child.getName());

                if (dpRel != null) {
                    model.add(model.createLiteralStatement(subject, model.createProperty((String)dpRel.getProperty("IRI")), child.getText()));
                }
                if (opRel != null) // TODO
                    logger.warn("opRel " + n.getProperty("IRI") + " " + child.getName() + " match " + opRel.getProperty("IRI"));

                continue;
            }

            Relationship opRel = opMatcher.match(child.getName());
            if (opRel != null) {
                for (Element subChild : child.getChildren()) {
                    logger.warn("Subchild " + subChild.getName());
                    RDFNode val = toRDF(subChild);
                    if (val == null)
                        continue;

                    try {
                        Resource o = (Resource) val;
                        model.add(model.createStatement(subject, model.createProperty((String)opRel.getProperty("IRI")), o));
                    } catch (ClassCastException e) {
                        // if opRel has value as literal then again we need to
                        // co-relate later
                        // TODO
                        model.add(model.createLiteralStatement(subject, model.createProperty((String)opRel.getProperty("IRI")), (Literal) val));
                    }
                }
            }
            
            // otherwise this could be an concept, in which case
            // we'll try to infer the relationship based on range
            IndexHits<Node> ranges = conceptIndex.query("CONCEPT", new QueryContext("*" + child.getName()).sortByScore());
            final Node cn = ranges.hasNext() ? ranges.next() : null;

            if (cn != null) {
                logger.warn(element.getName() + " -> Concept " + child.getName() + " " + cn.getProperty("IRI"));

                // try to find a compatible relationship
                Expander expander = StandardExpander.DEFAULT.add(ClusterSpace.PublicRelTypes.OBJECT_RELATIONSHIP, Direction.OUTGOING);

                PathFinder<Path> pf = GraphAlgoFactory.allSimplePaths(expander, 1);
                // TODO modify to find all paths and use some score again
                TraversalDescription trav = Traversal.description()
                    .breadthFirst()
                    .uniqueness(Uniqueness.RELATIONSHIP_GLOBAL)
                    .relationships(ClusterSpace.PublicRelTypes.EQUIVALENT_CLASS)
                    .relationships(ClusterSpace.PublicRelTypes.IS_A, Direction.OUTGOING);

                for (Node srcClazz : trav.traverse(n).nodes()) {
                    for (Node destClazz : trav.traverse(cn).nodes()) {
                        Path path = pf.findSinglePath(srcClazz, destClazz);
                        logger.warn(srcClazz.getProperty("IRI") + " ---> " + destClazz.getProperty("IRI") + " ? " + path);
                        if (path != null) {
                            logger.warn("Checking " + srcClazz.getProperty("IRI") + " " + path.lastRelationship().getProperty("IRI") + " " + destClazz.getProperty("IRI"));
                        }
                    }
                }
            }

            // child has subtags
            // they can either define a complete new instance
            // whose type we can infer from the subtag name
            // or we will have to infer based on tag collection
        }

        // if we have text nodes, this could be another thing requiring
        // co-relation
        // or something which only defines one 'primary' property
        // TODO

        return subject;
    }

    public Result perform() {
        Result r = new Result();

        conceptIndex = TurbulenceDriver.getClusterSpace().index().forNodes("conceptIndex");
        triples = new HashSet<Triple>();
        model = ModelFactory.createDefaultModel();

        try {
            SAXBuilder b = new SAXBuilder(XMLReaders.NONVALIDATING);
            Document document = b.build(input);
            for (Element el : document.getRootElement().getChildren()) {
                toRDF(el);
            }
            r.success = true;
        } catch (JDOMException e) {
            r.success = false;
            r.error = TurbulenceError.BAD_XML_DATA;
            r.message = e.getMessage();
        } catch (IOException e) {
            r.success = false;
            r.error = TurbulenceError.IO_ERROR;
            r.message = e.getMessage();
        }

        model.write(System.out, "RDF/XML-ABBREV");
        return r;
    }
}
