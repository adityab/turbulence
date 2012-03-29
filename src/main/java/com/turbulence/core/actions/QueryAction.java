package com.turbulence.core.actions;

import java.io.IOException;
import java.io.OutputStream;

import java.lang.String;

import java.util.*;

import javax.ws.rs.core.StreamingOutput;

import javax.ws.rs.WebApplicationException;

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
import com.turbulence.core.TurbulenceDriver;

import com.turbulence.util.AllColumnsIterator;

import me.prettyprint.cassandra.serializers.StringSerializer;

import me.prettyprint.hector.api.beans.HColumn;

import me.prettyprint.hector.api.factory.HFactory;

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
                logger.warn("RESULT set size " + rowKeys.size());

                for (String rowKey : rowKeys) {
                    SliceQuery<String, String, String> query = HFactory.createSliceQuery(TurbulenceDriver.getKeyspace(), StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
                    query.setKey(rowKey);
                    query.setColumnFamily("ConceptsInstancesData");
                    Iterator<HColumn<String, String>> it = new AllColumnsIterator<String, String>(query);
                    while (it.hasNext())
                        out.write(it.next().getValue().getBytes());
                }
            }
        };
	}

	public Result perform() {
	    throw new UnsupportedOperationException();
    }
}
