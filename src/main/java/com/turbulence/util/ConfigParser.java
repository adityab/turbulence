package com.turbulence.util;

import java.io.*;

import org.json.*;

public class ConfigParser {
    private String filename;
    private JSONObject root;

    /**
     * Reads a configuration from filename
     */
    private ConfigParser(String filename) throws ConfigParserException {
        filename = filename;
        try {
            JSONTokener tok = new JSONTokener(new BufferedReader(new InputStreamReader(new FileInputStream(filename))));
            root = new JSONObject(tok);
        } catch (JSONException e) {
            throw new ConfigParserException("Error parsing JSON in configuration file " + (new File(filename).getAbsolutePath()) + ": " + e.getMessage());
        } catch (IOException e) {
            throw new ConfigParserException("Error loading configuration file " + (new File(filename).getAbsolutePath()));
        }
    }

    public JSONObject getConfig() {
        return root;
    }

    public static ConfigParser getInstance() throws ConfigParserException {
        // TODO handle command line switch -c
        return new ConfigParser("./config.json");
    }
}
