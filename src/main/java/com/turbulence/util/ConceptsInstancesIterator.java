package com.turbulence.util;

import java.util.Iterator;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;

import com.hp.hpl.jena.util.iterator.NiceIterator;

import com.hp.hpl.jena.vocabulary.RDF;

import me.prettyprint.hector.api.beans.HColumn;

public class ConceptsInstancesIterator extends NiceIterator<Triple> {
    String type;
    Iterator<HColumn<String, String>> it;
    public ConceptsInstancesIterator(String type, Iterator<HColumn<String, String>> it) {
        this.type = type;
        this.it = it;
    }

    public boolean hasNext() {
        return it.hasNext();
    }

    public Triple next() {
        return Triple.create(Node.createURI(it.next().getValue()), Node.createURI(RDF.type.getURI()), Node.createURI(type));
    }
}
