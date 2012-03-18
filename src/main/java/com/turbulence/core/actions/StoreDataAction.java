package com.turbulence.core.actions;

import java.io.InputStream;
import java.io.StringWriter;

import java.util.Arrays;

import java.util.UUID;

import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;

import org.apache.commons.lang.UnhandledException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import me.prettyprint.cassandra.model.*;
import me.prettyprint.cassandra.serializers.*;
import me.prettyprint.cassandra.service.*;
import me.prettyprint.cassandra.service.template.*;
import me.prettyprint.hector.api.*;
import me.prettyprint.hector.api.ddl.*;
import me.prettyprint.hector.api.exceptions.*;
import me.prettyprint.hector.api.factory.HFactory;

public class StoreDataAction implements Action {
    private static final Log logger = LogFactory.getLog(StoreDataAction.class);

    private StringBuilder xpathExpr;
    private InputStream input;
    private XMLEventReader reader;
    private XMLOutputFactory outputFactory;
    ColumnFamilyTemplate<UUID, String> template;
    UUID elementID;
    protected StoreDataAction(InputStream in) {
        input = in;

        Cluster myCluster = HFactory.getOrCreateCluster("turbulence", "localhost:9160");
        ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition("Turbulence",
                "Main",
                ComparatorType.UTF8TYPE);
        KeyspaceDefinition kspd = myCluster.describeKeyspace("Turbulence");
        if (kspd == null) {
            KeyspaceDefinition newKeyspace = HFactory.createKeyspaceDefinition("Turbulence", ThriftKsDef.DEF_STRATEGY_CLASS, 1 /*TODO replication factor*/, Arrays.asList(cfDef));
            myCluster.addKeyspace(newKeyspace, true);
        }
        // FIXME: in a cluster we don't want this
        ConfigurableConsistencyLevel lvl = new ConfigurableConsistencyLevel();
        lvl.setDefaultReadConsistencyLevel(HConsistencyLevel.ONE);
        lvl.setDefaultWriteConsistencyLevel(HConsistencyLevel.ONE);
        Keyspace ksp = HFactory.createKeyspace("Turbulence", myCluster, lvl);

        template = new ThriftColumnFamilyTemplate<UUID, String>(ksp, "Main", UUIDSerializer.get(), StringSerializer.get());
    }

    public String recurse(XMLEvent n, String indent) throws XMLStreamException {
        assert n.isStartElement();

        StartElement el = n.asStartElement();
        xpathExpr.append("/" + el.getName().getPrefix() + ":" + el.getName().getLocalPart());
        String fullName = el.getName().getNamespaceURI() + el.getName().getLocalPart();

        StringWriter output = new StringWriter();
        XMLEventWriter writer = outputFactory.createXMLEventWriter(output);

        output.write("\n" + indent);
        writer.add(n);

        while (reader.hasNext() && !reader.peek().isEndElement()) {
            XMLEvent event = reader.nextEvent();
            if (event.isStartElement()) {
                output.write(recurse(event, indent + "  "));
            }
            else {
                writer.add(event);
            }
        }
        writer.add(reader.nextEvent()); // end element

        String data = output.toString();

        ColumnFamilyUpdater<UUID, String> up = template.createUpdater(elementID);
        up.setString(fullName, data);
        up.setString(fullName + "|" + xpathExpr, data);
        try {
            template.update(up);
        } catch (HectorException e) {
            throw new UnhandledException(e);
        }
        int index = xpathExpr.lastIndexOf("/");
        if (index != -1)
            xpathExpr.delete(index, xpathExpr.length());
        return data;
    }

    public Result perform() {
        Result r = new Result();

        try {
            reader = XMLInputFactory.newInstance().createXMLEventReader(input);
            outputFactory = XMLOutputFactory.newInstance();
            xpathExpr = new StringBuilder("/");

            // get inside the root element
            while (reader.hasNext() && !reader.nextEvent().isStartElement())
                ;

            while (reader.hasNext()) {
                XMLEvent event = reader.nextEvent();

                if (!event.isStartElement())
                    continue;

                elementID = UUID.randomUUID();
                recurse(event, "");
            }
            r.success = true;
        } catch (XMLStreamException e) {
            r.success = false;
            r.error = TurbulenceError.BAD_XML_DATA;
            r.message = "Error at line " + e.getLocation().getLineNumber() + " column " + e.getLocation().getColumnNumber();
        }
        return r;
    }
}
