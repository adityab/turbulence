package com.turbulence.util;

import java.io.File;

import org.apache.commons.codec.digest.DigestUtils;

import org.semanticweb.owlapi.model.*;

public class OntologySaver implements Runnable {
    private OWLOntology ontology;
    private File directory;
    private IRI canonicalIRI;

    public OntologySaver(IRI canon, OWLOntology ont, File baseDir) {
        ontology = ont;
        directory = baseDir;
        canonicalIRI = canon;
    }

    public void run() {
        String sha1 = DigestUtils.shaHex(canonicalIRI.toString());
        OWLOntologyManager manager = ontology.getOWLOntologyManager();
        try {
            manager.saveOntology(ontology, IRI.create(new File(directory, sha1)));
        } catch (OWLOntologyStorageException e) {
            assert false;
        }
    }
}
