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

import me.prettyprint.cassandra.serializers.StringSerializer;

import me.prettyprint.hector.api.beans.HColumn;

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
        // TODO: uncomment once debugging is done
        //if (q.getResultVars().isEmpty())
        //    return null;

        final ResultSet resultSet = exec.execSelect();

        return new StreamingOutput() {
            public void write(OutputStream out) throws IOException, WebApplicationException {
                Set<String> rowKeys = new HashSet<String>();

                while (resultSet.hasNext()) {
                    QuerySolution row = resultSet.next();

                    for (String var : resultSet.getResultVars()) {
                        try {
                            rowKeys.add(row.getResource(var).getURI());
                        } catch (ClassCastException e) {
                            rowKeys.add(row.getLiteral(var).getString());
                        }
                    }
                }
                logger.warn("RESULT set size " + rowKeys.size());

                // TODO: batch this
                for (String rowKey : rowKeys) {
                    HColumn<String, String> col = TurbulenceDriver.getInstanceDataTemplate().querySingleColumn(rowKey, "data", StringSerializer.get());
                    if (col != null && col.getValue() != null)
                        out.write(col.getValue().getBytes());
                }
            }
        };
	}

	public Result perform() {
	    throw new UnsupportedOperationException();
    }
}
