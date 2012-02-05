package com.turbulence.util;

import java.util.HashMap;

import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLOntologyIRIMapper;

public class OntologyMapper implements OWLOntologyIRIMapper {
    private HashMap<IRI,IRI> knownOntologies;

    public OntologyMapper() {
        knownOntologies = new HashMap<IRI, IRI>();
        knownOntologies.put(IRI.create("http://purl.org/dc/terms/"), IRI.create("file:/Users/nikhilmarathe/workspace/turbulence/data/dcterms.rdf"));
        knownOntologies.put(IRI.create("http://xmlns.com/foaf/0.1/"), IRI.create("file:/Users/nikhilmarathe/workspace/turbulence/data/foaf.rdf"));
        knownOntologies.put(IRI.create("http://nikhilism.com/test1"), IRI.create("file:/Users/nikhilmarathe/workspace/turbulence/data/test.rdf-xml.owl"));
    }

    public IRI getDocumentIRI(IRI ontologyIRI) {
        System.out.println("Cll " +ontologyIRI);
        return knownOntologies.get(ontologyIRI);
    }
}
