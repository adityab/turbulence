package com.turbulence.core.actions;

import java.io.IOException;
import java.io.OutputStream;

import java.lang.String;

import java.util.*;

import javax.ws.rs.core.StreamingOutput;

import javax.ws.rs.WebApplicationException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.hp.hpl.jena.graph.*;

import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.sparql.core.*;

import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.syntax.*;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;

import com.turbulence.core.ClusterSpaceJenaGraph;

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

        Model model = ModelFactory.createModelForGraph(new ClusterSpaceJenaGraph());
        QueryExecution exec = QueryExecutionFactory.create(q, model);
        final ResultSet resultSet = exec.execSelect();

        return new StreamingOutput() {
            public void write(OutputStream out) throws IOException, WebApplicationException {
                while (resultSet.hasNext()) {
                    QuerySolution row = resultSet.next();
                    for (String s : resultSet.getResultVars())
                        out.write(row.get(s).toString().getBytes());
                };
            }
        };
	}

	public Result perform() {
	    throw new UnsupportedOperationException();
    }
}
