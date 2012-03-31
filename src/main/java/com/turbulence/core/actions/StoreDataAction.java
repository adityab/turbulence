package com.turbulence.core.actions;

import java.io.InputStream;
import java.io.IOException;
import java.io.StringWriter;

import org.apache.commons.codec.digest.DigestUtils;

import org.apache.commons.lang.UnhandledException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import com.hp.hpl.jena.rdf.arp.ALiteral;
import com.hp.hpl.jena.rdf.arp.AResource;
import com.hp.hpl.jena.rdf.arp.ARP;
import com.hp.hpl.jena.rdf.arp.ARPEventHandler;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFWriter;
import com.hp.hpl.jena.rdf.model.Resource;

import com.hp.hpl.jena.vocabulary.RDF;

import com.turbulence.core.TurbulenceDriver;

import me.prettyprint.cassandra.service.template.*;
import me.prettyprint.hector.api.exceptions.*;

public class StoreDataAction implements Action, ARPEventHandler, ErrorHandler {
    private static final Log logger = LogFactory.getLog(StoreDataAction.class);

    private InputStream input;
    private ARP rdfParser;

    Model currentModel;
    Resource currentSubject = null;

    ColumnFamilyTemplate<String, String> conceptsTemplate;
    ColumnFamilyTemplate<String, String> instanceDataTemplate;
    SuperCfTemplate<String, String, String> spoTemplate;
    SuperCfTemplate<String, String, String> opsTemplate;

    protected StoreDataAction(InputStream in) {
        input = in;
        rdfParser = new ARP();
        rdfParser.getHandlers().setErrorHandler(this);
        rdfParser.getHandlers().setExtendedHandler(this);
        rdfParser.getHandlers().setNamespaceHandler(this);
        rdfParser.getHandlers().setStatementHandler(this);
        //rdfParser.getOptions().setLaxErrorMode();

        conceptsTemplate = TurbulenceDriver.getConceptsTemplate();
        instanceDataTemplate = TurbulenceDriver.getInstanceDataTemplate();
        spoTemplate = TurbulenceDriver.getSPODataTemplate();
        opsTemplate = TurbulenceDriver.getOPSDataTemplate();

        currentModel = ModelFactory.createDefaultModel();
    }

    public Result perform() {
        Result r = new Result();

        try {
            rdfParser.load(input);
            r.success = true;
        } catch (SAXException e) {
            r.success = false;
            r.error = TurbulenceError.BAD_XML_DATA;
            r.message = e.getMessage();
        } catch (IOException e) {
            r.success = false;
            r.error = TurbulenceError.IO_ERROR;
            r.message = e.getMessage();
        }
        return r;
    }

    public void error(SAXParseException exception) {
        logger.warn("E");
    }
    public void fatalError(SAXParseException exception) {
        logger.warn("FE");
    }
    public void warning(SAXParseException exception) {
        logger.warn("W");
    }

    public void startPrefixMapping(String prefix, String uri) {
        currentModel.setNsPrefix(prefix, uri);
    }

    public void endPrefixMapping(String prefix) {
        currentModel.removeNsPrefix(prefix);
    }

    public void statement(AResource subject, AResource predicate, AResource object) {
        if (currentSubject == null || !currentSubject.getURI().equals(subject.getURI())) {
            dumpRDFInstance();
            resetRDFInstance(subject.isAnonymous() ? subject.getAnonymousID() : subject.getURI());
        }

        currentSubject.addProperty(currentModel.createProperty(predicate.getURI()), currentModel.createResource(object.getURI()));

        if (predicate.getURI().equals(RDF.type.getURI())) {
            //logger.warn("Saving new entry of type " + object.getURI());
            ColumnFamilyUpdater<String, String> updater = conceptsTemplate.createUpdater(object.getURI());

            if (subject.isAnonymous())
                updater.setString(DigestUtils.md5Hex(subject.getAnonymousID()), subject.getAnonymousID());
            else
                updater.setString(DigestUtils.md5Hex(subject.getURI()), subject.getURI());

            try {
                conceptsTemplate.update(updater);
            } catch (HectorException e) {
                throw new UnhandledException(e);
            }
        }
        saveTriple(subject, predicate, object);
    }

    public void statement(AResource subject, AResource predicate, ALiteral object) {
        if (currentSubject == null || !currentSubject.getURI().equals(subject.getURI())) {
            dumpRDFInstance();
            resetRDFInstance(subject.isAnonymous() ? subject.getAnonymousID() : subject.getURI());
        }
        currentSubject.addProperty(currentModel.createProperty(predicate.getURI()), object.toString());
        saveTriple(subject, predicate, object);
    }

    private void saveTriple(AResource subject, AResource predicate, AResource object) {
        saveTriple(subject, predicate, object.isAnonymous() ? object.getAnonymousID() : object.getURI(), object.isAnonymous() ? "ANON" : "URI");
    }

    private void saveTriple(AResource subject, AResource predicate, ALiteral object) {
        saveTriple(subject, predicate, object.toString(),
                object.getDatatypeURI() == null ? "LITERAL" : "LITERAL|" + object.getDatatypeURI());
    }

    private void saveTriple(AResource subject, AResource predicate, String object, String objectType) {
        String rowKey = subject.isAnonymous() ? subject.getAnonymousID() : subject.getURI();
        SuperCfUpdater<String, String, String> spoUpdater = spoTemplate.createUpdater(rowKey, predicate.getURI());
        spoUpdater.setString(objectType + "|" + DigestUtils.md5Hex(object), object);
        try {
            spoTemplate.update(spoUpdater);
        } catch (HectorException e) {
            throw new UnhandledException(e);
        }

        if (!objectType.equals("URI"))
            return;

        // no point in storing inverse type relationships
        if (predicate.getURI().equals(RDF.type.getURI()))
            return;

        SuperCfUpdater<String, String, String> opsUpdater = opsTemplate.createUpdater(object, predicate.getURI());

        String value = subject.isAnonymous() ? subject.getAnonymousID() : subject.getURI();
        opsUpdater.setString("URI|" + DigestUtils.md5Hex(value), value);
        try {
            opsTemplate.update(opsUpdater);
        } catch (HectorException e) {
            throw new UnhandledException(e);
        }
    }

    private void dumpRDFInstance() {
        if (currentSubject == null || currentSubject.getPropertyResourceValue(RDF.type) == null)
            return;
        StringWriter w = new StringWriter();
        RDFWriter writer = currentModel.getWriter();
        writer.setProperty("allowBadURIs", true);
        writer.write(currentModel, w, null);
        //logger.warn("Will save dump " + w.toString());
        //logger.warn("to row " + currentSubject.getPropertyResourceValue(RDF.type).getURI() + " column " + currentSubject.getURI());
        ColumnFamilyUpdater<String, String> instanceDataUpdater = instanceDataTemplate.createUpdater(currentSubject.isAnon() ? currentSubject.getId().toString() : currentSubject.getURI());
        instanceDataUpdater.setString("data", w.toString());

        try {
            instanceDataTemplate.update(instanceDataUpdater);
        } catch (HectorException e) {
            throw new UnhandledException(e);
        }
    }

    private void resetRDFInstance(String subject) {
        currentModel.removeAll();
        currentSubject = currentModel.createResource(subject);
    }

    public void endBNodeScope(AResource bnode)
    {
    }

    public boolean discardNodesWithNodeID()
    {
        return false;
    }

    public void startRDF()
    {
    }

    public void endRDF()
    {
        // take care of the last subject
        dumpRDFInstance();
    }
}
