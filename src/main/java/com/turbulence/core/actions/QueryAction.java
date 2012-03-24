package com.turbulence.core.actions;

import java.io.IOException;
import java.io.OutputStream;

import java.lang.String;

import java.util.*;

import javax.ws.rs.core.StreamingOutput;

import javax.ws.rs.WebApplicationException;

import org.apache.commons.lang.UnhandledException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;

import com.turbulence.core.ClusterSpaceJenaGraph;

import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.UUIDSerializer;

import me.prettyprint.cassandra.service.ColumnSliceIterator;

import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;

import me.prettyprint.cassandra.service.ThriftKsDef;

import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.beans.Rows;

import me.prettyprint.hector.api.Cluster;

import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;

import me.prettyprint.hector.api.exceptions.HectorException;

import me.prettyprint.hector.api.factory.HFactory;

import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;

import me.prettyprint.hector.api.query.MultigetSliceQuery;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import me.prettyprint.hector.api.query.SliceQuery;

public class QueryAction implements Action {
    private static final Log logger = LogFactory.getLog(QueryAction.class);

    private final String query;
    /**
     * Constructs a new instance.
     */
    public QueryAction(String query)
    {
        this.query = query;
    }

	public StreamingOutput stream() {
        Query q = QueryFactory.create(query);

        if (q.getQueryType() != Query.QueryTypeSelect) {
            final String message = "<result><message>Cannot handle query " + query
                + ". Only SELECT supported.</message><error>"
                + TurbulenceError.QUERY_PARSE_FAILED + "</error></result>";
            return new StreamingOutput() {
                public void write(OutputStream out) throws IOException, WebApplicationException {
                    out.write(message.getBytes());
                }
            };
        }

        Model model = ModelFactory.createModelForGraph(new ClusterSpaceJenaGraph());
        QueryExecution exec = QueryExecutionFactory.create(q, model);
        final ResultSet resultSet = exec.execSelect();

        ColumnFamilyTemplate<String, UUID> template;
        Cluster myCluster = HFactory.getOrCreateCluster("turbulence", "localhost:9160");
        ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition("Turbulence",
                "Main",
                ComparatorType.UUIDTYPE);
        cfDef.setKeyValidationClass("UTF8Type");
        KeyspaceDefinition kspd = myCluster.describeKeyspace("Turbulence");
        if (kspd == null) {
            KeyspaceDefinition newKeyspace = HFactory.createKeyspaceDefinition("Turbulence", ThriftKsDef.DEF_STRATEGY_CLASS, 1 /*TODO replication factor*/, Arrays.asList(cfDef));
            myCluster.addKeyspace(newKeyspace, true);
        }
        // FIXME: in a cluster we don't want this
        ConfigurableConsistencyLevel lvl = new ConfigurableConsistencyLevel();
        lvl.setDefaultReadConsistencyLevel(HConsistencyLevel.ONE);
        lvl.setDefaultWriteConsistencyLevel(HConsistencyLevel.ONE);
        final Keyspace ksp = HFactory.createKeyspace("Turbulence", myCluster, lvl);

        template = new ThriftColumnFamilyTemplate<String, UUID>(ksp, "Main", StringSerializer.get(), UUIDSerializer.get());

        return new StreamingOutput() {
            public void write(OutputStream out) throws IOException, WebApplicationException {
                Set<String> rowKeys = new HashSet<String>();
                while (resultSet.hasNext()) {
                    QuerySolution row = resultSet.next();
                    List<String> urls = new ArrayList<String>();

                    for (String var : resultSet.getResultVars()) {
                        rowKeys.add(row.getResource(var).getURI());
                    }
                }
                for (String r : rowKeys) {
                    logger.warn("Key " + r);
                }

                for (String rowKey : rowKeys) {
                    SliceQuery<String, UUID, String> query = HFactory.createSliceQuery(ksp, StringSerializer.get(), UUIDSerializer.get(), StringSerializer.get());
                    query.setKey(rowKey);
                    query.setColumnFamily("Main");
                    UUID start = null;
                    int count = 100;
                    try {
                        while (true) {
                            query.setRange(start, null, false, count);
                            ColumnSlice<UUID, String> slice = query.execute().get();
                            List<HColumn<UUID, String>> columns = slice.getColumns();
                            int origSize = columns.size();
                            // next start is last one of this
                            // but we also drop it so that we don't have to
                            // serve duplicates
                            // but if number of columns we got is LESS than
                            // count already, then return all columns
                            if (origSize >= count)
                                start = columns.remove(columns.size()-1).getName();

                            for (HColumn<UUID, String> col : columns)
                                out.write(col.getValue().getBytes());

                            if (origSize < count)
                                break;
                        }
                    } catch (HectorException e) {
                        logger.warn("Exception processing " + rowKey + " start:" + start + " count:" + count + " -- " + e);
                        //throw new UnhandledException(e);
                    }
                }
            }
        };
	}

	public Result perform() {
	    throw new UnsupportedOperationException();
    }
}
