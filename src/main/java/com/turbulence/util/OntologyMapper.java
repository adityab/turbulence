package com.turbulence.util;

import java.io.File;

import org.apache.commons.codec.digest.DigestUtils;

import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLOntologyIRIMapper;

public class OntologyMapper implements OWLOntologyIRIMapper {
    private File directory;

    public OntologyMapper(File baseDir) {
        directory = baseDir;
    }

    public IRI getDocumentIRI(IRI ontologyIRI) {
        String sha1 = DigestUtils.shaHex(ontologyIRI.toString());
        File f = new File(directory, sha1);
        if (f.exists())
            return IRI.create(f);

        return null;
    }
}
