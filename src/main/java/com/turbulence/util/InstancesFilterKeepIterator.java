package com.turbulence.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.hp.hpl.jena.datatypes.BaseDatatype;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;

import com.hp.hpl.jena.rdf.model.AnonId;

import com.hp.hpl.jena.util.iterator.Filter;
import com.hp.hpl.jena.util.iterator.FilterIterator;
import com.hp.hpl.jena.util.iterator.FilterKeepIterator;
import com.hp.hpl.jena.util.iterator.NiceIterator;

import com.turbulence.core.TurbulenceDriver;

import me.prettyprint.cassandra.serializers.StringSerializer;

import me.prettyprint.hector.api.beans.HColumn;

import me.prettyprint.hector.api.factory.HFactory;

import me.prettyprint.hector.api.query.SubSliceQuery;

public class InstancesFilterKeepIterator extends NiceIterator<Triple> {
    private static final Log logger =
        LogFactory.getLog(InstancesFilterKeepIterator.class);

    private String rowKey;
    private String predicate;
    private FilterIterator<HColumn<String, String>> it;
    public InstancesFilterKeepIterator(String rowKey, String predicate, Filter<HColumn<String, String>> filter, String columnFamily) {
        this.rowKey = rowKey;
        this.predicate = predicate;
        // load from SPO table
        SubSliceQuery<String, String, String, String> query
            = HFactory.createSubSliceQuery(TurbulenceDriver.getKeyspace(),
                    StringSerializer.get(), StringSerializer.get(),
                    StringSerializer.get(), StringSerializer.get());
        query.setKey(rowKey);
        query.setColumnFamily(columnFamily);
        query.setSuperColumn(predicate);
        AllSubColumnsIterator<String, String> allSubColumns = new AllSubColumnsIterator<String, String>(query);
        it = new FilterKeepIterator<HColumn<String, String>>(filter, allSubColumns);
    }

    public boolean hasNext() {
        return it.hasNext();
    }

    // TODO refactor
    public Triple next() {
        HColumn<String, String> col = it.next();
        Node subject = Node.createURI(rowKey);
        Node predicate = Node.createURI(this.predicate);
        Node object = null;
        if (col.getName().startsWith("URI"))
            object = Node.createURI(col.getValue());
        else if (col.getName().startsWith("ANON"))
            object = Node.createAnon(AnonId.create(col.getValue()));
        else if (col.getName().startsWith("LITERAL")) {
            String name = col.getName();
            try {
                String type = name.substring(name.indexOf("|")+1, name.lastIndexOf("|"));
                object = Node.createLiteral(col.getValue(), new BaseDatatype(type));
            } catch (IndexOutOfBoundsException e) {
                object = Node.createLiteral(col.getValue());
            }
        }
        else {
            // only for backwards compatibility, because
            // the OPS column in currently screwed
            object = Node.createURI(col.getValue());
        }

        return Triple.create(subject, predicate, object);
    }
}
