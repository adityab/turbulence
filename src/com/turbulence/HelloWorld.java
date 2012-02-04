package com.turbulence;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.UnknownHostException;

import java.util.HashSet;
import java.util.Set;

import javax.xml.parsers.*;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.util.FileUtils;
import org.semanticweb.owlapi.io.*;
import org.semanticweb.owlapi.model.*;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.semanticweb.owlapi.apibinding.*;

import org.semanticweb.owlapi.reasoner.NodeSet;
import org.semanticweb.owlapi.reasoner.Node;
import org.semanticweb.owlapi.util.SimpleIRIMapper;
import org.semanticweb.owlapi.util.*;
import com.clarkparsia.pellet.owlapiv3.*;

public class HelloWorld {
    private static String DB_PATH = "data/neo4j-test-schema.db";
    private static String DC_URI = "http://purl.org/dc/terms";
    private static String FOAF_URI = "http://xmlns.com/foaf/0.1/";
    private static String TEST_URI = "http://nikhilism.com/test1";

    private static enum Rels implements RelationshipType {
        IS_A,
        ASSOCIATION
    }

    public static void main(String[] args) {
        EmbeddedGraphDatabase db = new EmbeddedGraphDatabase(DB_PATH);
        registerShutdownHook(db);

        try {
            IRI dc = IRI.create(DC_URI);
            IRI foaf = IRI.create(FOAF_URI);
            IRI testO = IRI.create(TEST_URI);

            OWLOntologyManager oom = OWLManager.createOWLOntologyManager();
            oom.addIRIMapper(new SimpleIRIMapper(dc, IRI.create("file:/Users/nikhilmarathe/workspace/turbulence/data/dcterms.rdf")));
            oom.addIRIMapper(new SimpleIRIMapper(foaf, IRI.create("file:/Users/nikhilmarathe/workspace/turbulence/data/foaf.rdf")));
            oom.addIRIMapper(new SimpleIRIMapper(testO, IRI.create("file:/Users/nikhilmarathe/workspace/turbulence/data/test.rdf-xml.owl")));

            OWLOntology ont = oom.loadOntology(testO);

            /*Set<OWLClassExpression> classes = ont.getNestedClassExpressions();
            for (OWLClassExpression ex : classes) {
                OWLClass c = ex.asOWLClass();

                //c.accept(vis);
            }
            ont.accept(vis);
                /*Set<OWLAxiom> axs = ont.getReferencingAxioms(c);
                for (OWLAxiom ax : axs) {
                    System.out.println(ax.getAxiomType().toString());
                    if (ax.getAxiomType() == AxiomType.SUBCLASS_OF) {
                        OWLSubClassOfAxiom soax = (OWLSubClassOfAxiom) ax;
                        System.out.println(soax.getSubClass().toString() + " is subclass of " + soax.getSuperClass().toString());
                    }
                    else if (ax.getAxiomType() == AxiomType.DATA_PROPERTY_DOMAIN) {
                        OWLDataPropertyAxiom dax = (OWLDataPropertyAxiom) ax;
                        System.out.println("Data property " + 
                    }
                }
            }*/

            PelletReasoner r = PelletReasonerFactory.getInstance().createReasoner(ont);
            OWLClass mh = oom.getOWLDataFactory().getOWLClass(IRI.create(TEST_URI + "#Maharashtrian"));
            Set<Node<OWLClass>> sup = r.getSuperClasses(mh, true).getNodes();

            for (Node<OWLClass> node : sup) {
                OWLClass c = node.getRepresentativeElement();
                System.out.println(c.getIRI());
            }
        } catch (OWLOntologyCreationIOException e) {
            System.out.println(e.getMessage());
        } catch (UnparsableOntologyException e) {
            System.out.println(e.getMessage());
        } catch (UnloadableImportException e) {
            System.out.println(e.getMessage());
        } catch (OWLOntologyCreationException e) {
            System.out.println(e.getMessage());
        }
    }

    private static void registerShutdownHook(final GraphDatabaseService db) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                db.shutdown();
            }
        });
    }
}
