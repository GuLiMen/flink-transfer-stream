package cn.qtech.bigdata.prop;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Properties;

public class PropertiesManager implements Serializable {
    private static final PropertiesManager PM = new PropertiesManager();
    private Properties storage = null;

    public static PropertiesManager getInstance() {
        return PM;
    }

    /**
     * @param filename 从给定路径读取属性文件
     * @throws IOException
     */
    public static void loadProps(String filename) throws IOException {
        if (PM.storage == null) {
            synchronized (PropertiesManager.class) {
                if (PM.storage == null) {
                    PM.storage = new Properties();
                    if (filename != null) {
                        try (InputStream propStream = PropertiesManager.class.getResourceAsStream(filename)) {
                            PM.storage.load(propStream);
                        }
                    } else {
                        throw new IllegalArgumentException("Did not load any properties since the property file is not specified");
                    }
                }
            }
        } else {
            if (filename != null) {
                try (InputStream propStream = PropertiesManager.class.getResourceAsStream(filename)) {
                    PM.storage.load(propStream);
                }
            } else {
                throw new IllegalArgumentException("Did not load any properties since the property file is not specified");
            }
        }
    }

    /**
     * read string value
     */
    public String getString(Object key) {
        return get(key).toString();
    }

    /**
     * read Integer value
     */
    public Integer getInt(Object key) {
        return Integer.valueOf(get(key).toString());
    }

    /**
     * read Double value
     */
    public Double getDouble(Object key) {
        return Double.valueOf(get(key).toString());
    }

    /**
     * read from properties
     */
    public Object get(Object key) {
        if (PM.storage == null)
            throw new IllegalStateException("can not init properties config");
        Object prop = PM.storage.get(key);
        if (prop == null)
            throw new IllegalArgumentException("can not found properties key " + key);
        return prop;
    }
}
