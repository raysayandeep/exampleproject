package com.legstar.pdi;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.vfs.FileObject;
import org.apache.commons.vfs.FileSystemException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.plugins.KettleURLClassLoader;
import org.pentaho.di.core.plugins.PluginFolder;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.core.row.RowMetaInterface;
import org.scannotation.AnnotationDB;

import com.legstar.coxb.CobolBindingException;
import com.legstar.coxb.ICobolComplexBinding;
import com.legstar.coxb.convert.simple.CobolSimpleConverters;
import com.legstar.coxb.host.HostException;
import com.legstar.coxb.impl.visitor.FlatCobolUnmarshalVisitor;
import com.legstar.coxb.transform.HostTransformStatus;
import com.legstar.coxb.util.BindingUtil;

/**
 * This is a Mediator between the PDI API and the LegStar PDI. </p> The idea is
 * to keep the PDI step code as simple as possible and not leak too much LegStar
 * specific code into the step code.
 * 
 */
public class Cob2Pdi {

    /** An identifier for our lib class loader. */
    public static final String LIB_CLASSLOADER_NAME = "legstar.pdi.lib";

    /** The default plugin location. */
    public static final String DEFAULT_PLUGIN_FOLDER = "plugins/legstar.pdi.zosfile";

    /** A system property that might hold an alternate plugin location. */
    public static final String PLUGIN_FOLDER_PROPERTY = "legstar.pdi.plugin.folder";

    /** The relative location the lib folder (known to PDI). */
    public static final String LIB_FOLDER = "lib";

    /** The relative location our private user folder to hold generated jars. */
    public static final String USER_FOLDER = "user";

    /** The relative location of our private configuration folder. */
    public static final String CONF_FOLDER = "conf";

    /** The configuration file name. */
    public static final String CONF_FILE_NAME = "cob2trans.properties";

    /** The JAXB/COBOL Legstar classes should be annotated with this. */
    public static final String LEGSTAR_ANNOTATIONS = "com.legstar.coxb.CobolElement";

    /**
     * Utility class. No instantiation.
     */
    private Cob2Pdi() {

    }

    /*
     * ------------------------------------------------------------------------
     * Runtime Transformer execution.
     * ------------------------------------------------------------------------
     */
    /**
     * Create an instance of COBOL binding for a given JAXB root class name.
     * Assumes binding classes were generated for this JAXB class.
     * 
     * @param jaxbQualifiedClassName the JAXB class name
     * @return a new instance of the COBOL binding class
     * @throws KettleException if bindings cannot be created
     */
    public static ICobolComplexBinding newCobolBinding(
            final String jaxbQualifiedClassName) throws KettleException {
        try {
            return BindingUtil.newTransformers(jaxbQualifiedClassName)
                    .getHostToJava().newBinding();
        } catch (CobolBindingException e) {
            throw new KettleException(e);
        }
    }

    /**
     * Allocates a byte array large enough to accommodate the largest host
     * record.
     * 
     * @param cobolBinding the COBOL binding for the record
     * @return a byte array large enough for the largest record
     */
    public static byte[] newHostRecord(final ICobolComplexBinding cobolBinding) {
        return new byte[cobolBinding.getByteLength()];
    }

    /**
     * Creates a PDI output row from host data.
     * 
     * @param outputRowMeta the output row meta data.
     * @param cobolBinding COBOL binding for the row
     * @param hostRecord the host data
     * @param hostCharset the host character set
     * @param status additional info on the COBOL to Java transformation process
     * @return a PDI output row of data
     * @throws KettleException if transformation fails
     */
    public static Object[] toOutputRowData(
            final RowMetaInterface outputRowMeta,
            final ICobolComplexBinding cobolBinding, final byte[] hostRecord,
            final String hostCharset, final HostTransformStatus status)
            throws KettleException {
        try {
            int expectedOutputRows = outputRowMeta.getFieldNames().length;
            FlatCobolUnmarshalVisitor cev = new FlatCobolUnmarshalVisitor(
                    hostRecord, 0, new CobolSimpleConverters());
            cobolBinding.accept(cev);
            status.setHostBytesProcessed(cev.getOffset());

            List<Object> objects = new ArrayList<Object>();
            addValues(objects, cev.getKeyValues().values());

            /*
             * PDI does not support variable size arrays. Need to fill all
             * columns.
             */
            for (int i = objects.size(); i < expectedOutputRows; i++) {
                objects.add(null);
            }

            return objects.toArray(new Object[objects.size()]);

        } catch (CobolBindingException e) {
            throw new KettleException(e);
        } catch (HostException e) {
            throw new KettleException(e);
        }
    }

    /**
     * There are slight differences between PDI and LegStar object types that we
     * resolve here.
     * 
     * @param objects the PDI objects
     * @param values the LegStar values
     */
    public static void addValues(List<Object> objects, Collection<Object> values) {
        for (Object value : values) {
            if (value instanceof Short) {
                objects.add(new Long((Short) value));
            } else if (value instanceof Integer) {
                objects.add(new Long((Integer) value));
            } else {
                objects.add(value);
            }
        }

    }

    /*
     * ------------------------------------------------------------------------
     * Class path handling.
     * ------------------------------------------------------------------------
     */
    /**
     * Create a thread context class loader with an additional jar containing
     * Transformer and JAXB classes.
     * <p/>
     * This is expensive so avoid doing that several times for the same thread.
     * <p/>
     * The jar is expected to live in a user sub folder under our plugin where
     * it was either generated or manually copied from elsewhere.
     * 
     * @param clazz the class that attempts to set the class loader
     * @param jarFileName the jar file containing the JAXB classes. If null this
     *        is a no op
     * @throws KettleException if classloader fails
     */
    public static void setTransformerClassLoader(Class<?> clazz,
            final String jarFileName) throws KettleException {

        if (jarFileName == null) {
            return;
        }

        String classLoaderName = LIB_CLASSLOADER_NAME + '.' + jarFileName;
        ClassLoader tccl = Thread.currentThread().getContextClassLoader();
        if (tccl instanceof KettleURLClassLoader) {
            if (((KettleURLClassLoader) tccl).getName().equals(classLoaderName)) {
                // Already setup
                return;
            }
        }

        try {
            File jarFile = new File(getPluginUserLocation() + '/' + jarFileName);
            ClassLoader parent = clazz.getClassLoader();
            KettleURLClassLoader cl = new KettleURLClassLoader(
                    new URL[] { jarFile.toURI().toURL() }, parent,
                    classLoaderName);
            Thread.currentThread().setContextClassLoader(cl);
        } catch (Exception e) {
            throw new KettleException(e);
        }
    }

    /**
     * Convenience method to get our private user folder.
     * 
     * @return a kettle plugin folder
     */
    public static PluginFolder getPluginUserFolder() {
        return new PluginFolder(getPluginUserLocation(), false, false);
    }

    /**
     * Find out where we are installed within Kettle.
     * <p/>
     * If we are registered as a plugin (during integration tests or production)
     * then we get the location of our plugin from the registry.
     * <p/>
     * If the plugin is not registered with PDI check for a system property
     * otherwise fallback to "user.dir".
     * 
     * @return the location where the plugin installled
     */
    public static String getPluginLocation() {
        String pluginLocation = null;
        PluginInterface plugin = PluginRegistry.getInstance().findPluginWithId(
                StepPluginType.class, "com.legstar.pdi.zosfile");
        if (plugin != null && plugin.getPluginDirectory() != null) {
            pluginLocation = plugin.getPluginDirectory().getPath();
        } else if (System.getProperty(PLUGIN_FOLDER_PROPERTY) != null) {
            pluginLocation = System.getProperty(PLUGIN_FOLDER_PROPERTY);
        } else {
            pluginLocation = System.getProperty("user.dir") + '/'
                    + DEFAULT_PLUGIN_FOLDER;
        }
        return pluginLocation;
    }

    /**
     * @return the location of our private user folder under the plugin
     */
    public static String getPluginUserLocation() {
        return getPluginLocation() + '/' + USER_FOLDER;
    }

    /**
     * @return the location of our private configuration folder under the plugin
     */
    public static String getPluginConfLocation() {
        return (getPluginLocation() == null) ? null : getPluginLocation() + '/'
                + CONF_FOLDER;
    }

    /**
     * @return the location of the lib folder (known to PDI) under the plugin
     */
    public static String getPluginLibLocation() {
        return getPluginLocation() + '/' + LIB_FOLDER;
    }

    /**
     * @return a classpath built with jars from the user lib folder
     */
    public static String getLibClassPath() {
        return getClasspath(Cob2Pdi.getPluginLibLocation());
    }

    /**
     * Creates a classpath by concatenating all jar files found under a
     * particular folder..
     * 
     * @param folderPath the location where jar files sit
     * @return a classpath usable to start java
     */
    @SuppressWarnings("unchecked")
    public static String getClasspath(final String folderPath) {
        Collection<File> jarFiles = FileUtils.listFiles(new File(folderPath),
                new String[] { "jar" }, false);
        StringBuilder sb = new StringBuilder();
        boolean next = false;
        for (File jarFile : jarFiles) {
            if (next) {
                sb.append(File.pathSeparator);
            } else {
                next = true;
            }
            sb.append(jarFile.getPath());
        }
        return sb.toString();
    }

    /*
     * ------------------------------------------------------------------------
     * COBOL-annotated JAXB classes discovery and jar association.
     * ------------------------------------------------------------------------
     */
    /**
     * Fetches all available COBOL-annotated JAXB class names from the user
     * sub-folder.
     * 
     * @return null if no jars found in user sub folder, otherwise all
     *         COBOL-annotated JAXB classes along with the jar file name which
     *         contains that class. Each item in the list is formatted like so:
     *         className[jarFileName]
     * @throws KettleFileException in case of read failure on the jar files
     */
    public static List<String> getAvailableCompositeJaxbClassNames()
            throws KettleFileException {
        try {
            List<String> compositeJaxbclassNames = null;
            FileObject[] fileObjects = getPluginUserFolder().findJarFiles();
            if (fileObjects != null && fileObjects.length > 0) {
                compositeJaxbclassNames = new ArrayList<String>();
                for (FileObject fileObject : fileObjects) {
                    AnnotationDB annotationDB = new AnnotationDB();
                    annotationDB.scanArchives(fileObject.getURL());
                    Set<String> classNames = annotationDB.getAnnotationIndex()
                            .get(LEGSTAR_ANNOTATIONS);
                    if (classNames != null) {
                        for (String className : classNames) {
                            compositeJaxbclassNames
                                    .add(getCompositeJaxbClassName(className,
                                            fileObject.getName().getBaseName()));
                        }
                    }
                }
            }
            return compositeJaxbclassNames;
        } catch (FileSystemException e) {
            throw new KettleFileException(e);
        } catch (IOException e) {
            throw new KettleFileException(e);
        }
    }

    /**
     * Compose a name concatenating class name and containing jar file name.
     * 
     * @param qualifiedClassName the class name
     * @param jarFileName the containing jar file name
     * @return a string such as qualifiedClassName[jarFileName]
     */
    public static String getCompositeJaxbClassName(
            final String qualifiedClassName, final String jarFileName) {
        return qualifiedClassName + "[" + jarFileName + "]";
    }

    /**
     * Strip the containing jar file name and return the bare class name.
     * 
     * @param compositeJAXBClassName a string concatenating jar file name and
     *        class name
     * @return the bare class name part of the composite class name
     */
    public static String getJaxbClassName(final String compositeJAXBClassName) {
        int i = compositeJAXBClassName.indexOf('[');
        if (i > 0) {
            return compositeJAXBClassName.substring(0, i);
        }
        return compositeJAXBClassName;
    }

    /**
     * Strip the class name and return the jar file name.
     * 
     * @param compositeJAXBClassName a string concatenating jar file name and
     *        class name
     * @return the bare jar file name of the composite class name
     */
    public static String getJarFileName(final String compositeJAXBClassName) {
        int i = compositeJAXBClassName.indexOf('[');
        if (i > 0) {
            return compositeJAXBClassName.substring(i + 1,
                    compositeJAXBClassName.length() - 1);
        }
        return null;
    }

}
