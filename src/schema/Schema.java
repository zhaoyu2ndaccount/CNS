package schema;

import mongo.MongoApp;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class Schema {

    public static List<String> attributes;

    /**
     *
     * @param attributes
     */
    public Schema(List<String> attributes) {
        this.attributes = attributes;
    }

    /**
     *
     * @param path
     */
    public Schema(String path) {
        this.attributes = new ArrayList<>();
        File f = new File(path);
        if (f.exists()) {
            // TODO: read in attributes from a config file

        } else {
            // construct a default schema
            for (int i=0; i<MongoApp.num_attributes; i++) {
                attributes.add(MongoApp.ATTR_PREFIX+i);
            }
        }
    }

    /**
     * Specify a list
     * @param numAttr
     */
    public Schema(int numAttr) {
        this.attributes = new ArrayList<>();
        for (int i=0; i<numAttr; i++) {
            attributes.add(MongoApp.ATTR_PREFIX+i);
        }
    }

}
