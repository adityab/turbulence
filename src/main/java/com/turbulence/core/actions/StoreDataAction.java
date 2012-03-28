package com.turbulence.core.actions;

import java.io.InputStream;
import java.io.IOException;
import java.io.StringWriter;

import java.util.Arrays;

import java.util.UUID;

import org.apache.commons.codec.digest.DigestUtils;

import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamException;

import org.apache.commons.lang.UnhandledException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import com.hp.hpl.jena.rdf.arp.ALiteral;
import com.hp.hpl.jena.rdf.arp.AResource;
import com.hp.hpl.jena.rdf.arp.ARP;
import com.hp.hpl.jena.rdf.arp.NamespaceHandler;
import com.hp.hpl.jena.rdf.arp.StatementHandler;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;

import com.turbulence.core.TurbulenceDriver;

import me.prettyprint.cassandra.model.*;
import me.prettyprint.cassandra.serializers.*;
import me.prettyprint.cassandra.service.*;
import me.prettyprint.cassandra.service.template.*;
import me.prettyprint.hector.api.*;
import me.prettyprint.hector.api.ddl.*;
import me.prettyprint.hector.api.exceptions.*;
import me.prettyprint.hector.api.factory.HFactory;

public class StoreDataAction implements Action, ErrorHandler, NamespaceHandler, StatementHandler {
    private static final Log logger = LogFactory.getLog(StoreDataAction.class);

    private InputStream input;
    private ARP rdfParser;
    private XMLOutputFactory outputFactory;

    Model currentModel = null;
    Resource currentSubject = null;

    ColumnFamilyTemplate<String, String> conceptsTemplate;
    ColumnFamilyTemplate<String, String> conceptsInstanceDataTemplate;
    SuperCfTemplate<String, String, String> triplesTemplate;
    protected StoreDataAction(InputStream in) {
        input = in;
        rdfParser = new ARP();
        rdfParser.getHandlers().setErrorHandler(this);
        rdfParser.getHandlers().setNamespaceHandler(this);
        rdfParser.getHandlers().setStatementHandler(this);

        conceptsTemplate = TurbulenceDriver.getConceptsTemplate();
        conceptsInstanceDataTemplate = TurbulenceDriver.getConceptsInstanceDataTemplate();
        triplesTemplate = TurbulenceDriver.getTriplesTemplate();
    }

        /*ColumnFamilyUpdater<String, UUID> up = template.createUpdater(fullName);
        up.setString(UUID.randomUUID(), data);
        try {
            template.update(up);
        } catch (HectorException e) {
            throw new UnhandledException(e);
        }*/

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
    }
    public void fatalError(SAXParseException exception) {
    }
    public void warning(SAXParseException exception) {
    }

    public void startPrefixMapping(String prefix, String uri) {
        logger.warn("Started prefix matching " + prefix + ": " + uri);
    }

    public void endPrefixMapping(String prefix) {
    }

    public void statement(AResource subject, AResource predicate, AResource object) {
        if (currentSubject == null || !currentSubject.getURI().equals(subject.getURI())) {
            dumpRDFInstance();
            resetRDFInstance(subject.getURI());
        }

        currentSubject.addProperty(currentModel.createProperty(predicate.getURI()), currentModel.createResource(object.getURI()));

        if (predicate.getURI().equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) {
            logger.warn("Saving new entry of type " + object.getURI());
            ColumnFamilyUpdater<String, String> updater = conceptsTemplate.createUpdater(object.getURI());
            updater.setString(DigestUtils.md5Hex(subject.getURI()), subject.getURI());

            try {
                //conceptsTemplate.update(updater);
            } catch (HectorException e) {
                throw new UnhandledException(e);
            }
        }
        else {
            saveTriple(subject.getURI(), predicate.getURI(), object.getURI());
        }
        logger.warn("statementAResource: " + subject + " " + predicate + " " + object);
    }

    public void statement(AResource subject, AResource predicate, ALiteral object) {
        if (currentSubject == null || !currentSubject.getURI().equals(subject.getURI())) {
            dumpRDFInstance();
            resetRDFInstance(subject.getURI());
        }
        currentSubject.addProperty(currentModel.createProperty(predicate.getURI()), object.toString());
        saveTriple(subject.getURI(), predicate.getURI(), object.toString());
        logger.warn("statementALiteral: " + subject + " " + predicate + " " + object);
    }

    private void saveTriple(String subject, String predicate, String object) {
        logger.warn("Saving triple with row key " + subject + " SCol: " + predicate + " colName: " + DigestUtils.md5Hex(object) + " colValue: " + object);
        SuperCfUpdater<String, String, String> updater = triplesTemplate.createUpdater(subject, predicate);
        updater.setString(DigestUtils.md5Hex(object), object);
        try {
            //updater.update();
        } catch (HectorException e) {
            throw new UnhandledException(e);
        }
    }

    private void dumpRDFInstance() {
        if (currentSubject == null)
            return;
        StringWriter w = new StringWriter();
        currentModel.write(w);
        logger.warn("Will save dump " + w.toString());
        logger.warn("to row " + currentSubject.getPropertyResourceValue(currentModel.createProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")).getURI());
        ColumnFamilyUpdater<String, String> updater = conceptsInstanceDataTemplate.createUpdater(currentSubject.getPropertyResourceValue(currentModel.createProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")).getURI());

        updater.setString(currentSubject.getURI(), w.toString());
        try {
            //updater.update();
        } catch (HectorException e) {
            throw new UnhandledException(e);
        }
    }

    private void resetRDFInstance(String subject) {
        currentModel = ModelFactory.createDefaultModel();
        currentSubject = currentModel.createResource(subject);
    }
}
