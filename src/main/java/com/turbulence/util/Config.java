package com.turbulence.util;

import java.io.*;

import org.json.*;

public class Config extends JSONObject {
    /**
     * Reads a configuration from filename
     */
    private Config(JSONTokener tok) throws JSONException {
        super(tok);
    }

    public JSONObject getSection(String name) {
        try {
            return getJSONObject(name);
        } catch (JSONException e) {
            return null;
        }
    }

    public static Config getInstance() throws ConfigParseException {
        // TODO handle command line switch -c
        String filename = "./config.json";
        try {
            JSONTokener tok = new JSONTokener(new BufferedReader(new InputStreamReader(new FileInputStream(filename))));
            return new Config(tok);
        } catch (JSONException e) {
            throw new ConfigParseException("Error parsing JSON in configuration file " + (new File(filename).getAbsolutePath()) + ": " + e.getMessage());
        } catch (IOException e) {
            throw new ConfigParseException("Error loading configuration file " + (new File(filename).getAbsolutePath()));
        }
    }
}
