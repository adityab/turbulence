package com.turbulence.util;

import java.io.*;

import org.json.*;

public class Config extends JSONObject {
    /**
     * Reads a configuration from filename
     */
    private Config(String filename) throws ConfigParseException {
        try {
            JSONTokener tok = new JSONTokener(new BufferedReader(new InputStreamReader(new FileInputStream(filename))));
            super(tok);
        } catch (JSONException e) {
            throw new ConfigParseException("Error parsing JSON in configuration file " + (new File(filename).getAbsolutePath()) + ": " + e.getMessage());
        } catch (IOException e) {
            throw new ConfigParseException("Error loading configuration file " + (new File(filename).getAbsolutePath()));
        }
    }

    public static Config getInstance() throws ConfigParseException {
        // TODO handle command line switch -c
        return new Config("./config.json");
    }
}
