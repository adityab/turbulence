package com.turbulence.core.actions;

import java.util.Arrays;

import java.util.logging.Logger;

import java.util.List;
import java.util.UUID;

import org.w3c.dom.Document;
import org.jdom.*;
import org.jdom.input.DOMBuilder;
import org.jdom.output.XMLOutputter;

import me.prettyprint.cassandra.model.*;
import me.prettyprint.cassandra.serializers.*;
import me.prettyprint.cassandra.service.*;
import me.prettyprint.cassandra.service.template.*;
import me.prettyprint.hector.api.*;
import me.prettyprint.hector.api.ddl.*;
import me.prettyprint.hector.api.exceptions.*;
import me.prettyprint.hector.api.factory.HFactory;

public class StoreDataAction implements Action {
    private Logger logger;
    private StringBuilder xpathExpr;
    private org.jdom.Document xmlDocument;
    ColumnFamilyTemplate<UUID, String> template;
    UUID elementID;
    protected StoreDataAction(Document data) {
        logger = Logger.getLogger(this.getClass().getName());
        xmlDocument = new DOMBuilder().build(data);

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

    public void recurse(Element n, String indent) {
        xpathExpr.append("/" + n.getQualifiedName());
        String fullName = n.getNamespaceURI()+n.getName();

        ColumnFamilyUpdater<UUID, String> up = template.createUpdater(elementID);

        String data;
        if (n.getChildren().size() == 0) {
            data = new XMLOutputter().outputString(n);
        }
        else {
            Element el = (Element) n.clone();
            el.removeContent();
            data = new XMLOutputter().outputString(el);
        }
        up.setString(fullName, data);
        up.setString(fullName + "|" + xpathExpr, data);
        try {
            template.update(up);
        } catch (HectorException e) {
            System.err.println(e);
            // TODO
        }
        System.err.println(indent + "expr: " + xpathExpr);
        List l = n.getChildren();
        for (int i = 0; i < l.size(); i++) {
            recurse((Element)l.get(i), indent + "  ");
        }
        int index = xpathExpr.lastIndexOf("/");
        if (index != -1)
            xpathExpr.delete(index, xpathExpr.length());
    }

    public Result perform() {
        xpathExpr = new StringBuilder("/");
        for (int i = 0; i < xmlDocument.getRootElement().getChildren().size(); i++) {
            elementID = UUID.randomUUID();
            recurse((Element)xmlDocument.getRootElement().getChildren().get(i), "");
        }
        return new Result();
    }
}
