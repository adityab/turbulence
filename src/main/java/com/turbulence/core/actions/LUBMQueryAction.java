package com.turbulence.core.actions;

import java.io.IOException;
import java.io.OutputStream;

import java.util.HashSet;
import java.util.Set;

import javax.ws.rs.core.StreamingOutput;

import javax.ws.rs.WebApplicationException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

import com.turbulence.core.ClusterSpaceJenaGraph;
import com.turbulence.core.TurbulenceDriver;

import me.prettyprint.cassandra.serializers.StringSerializer;

import me.prettyprint.hector.api.beans.HColumn;

public class LUBMQueryAction implements Action {
    private static final Log logger = LogFactory.getLog(LUBMQueryAction.class);

    private final String query;
    /**
     * Constructs a new instance.
     */
    public LUBMQueryAction(String query)
    {
        this.query = query;
    }

	public Result perform() {
	    logger.warn(query);
        Query q = QueryFactory.create(query);

        if (q.getQueryType() != Query.QueryTypeSelect) {
            Result r = new Result();
            r.error = TurbulenceError.QUERY_PARSE_FAILED;
            r.message = "Only SELECT supported";
            return r;
        }

        Model model = ModelFactory.createModelForGraph(new ClusterSpaceJenaGraph());
        QueryExecution exec = QueryExecutionFactory.create(q, model);
        // TODO: uncomment once debugging is done
        //if (q.getResultVars().isEmpty())
        //    return null;

        final ResultSet resultSet = exec.execSelect();
        long count = 0;
        while (resultSet.hasNext()) {
            resultSet.next();
            count++;
        }

        Result r = new Result();
        r.success = true;
        r.message = Long.toString(count);
        return r;
    }
}
