// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import org.datanucleus.jdo.JDOPersistenceManagerFactory;
import org.datanucleus.jdo.JDOPersistenceManager;

import java.util.Map;
import java.util.Properties;
import java.util.HashMap;
import java.util.Enumeration;

import javax.jdo.PersistenceManagerFactory;
import javax.jdo.JDOHelper;

/**
 * Extension to {@link JDOPersistenceManagerFactory} that allows us to
 * instantiate instances of {@link DatastoreJDOPersistenceManager} instead of
 * {@link JDOPersistenceManager}.
 *
 * @author Max Ross <maxr@google.com>
 */
public class DatastoreJDOPersistenceManagerFactory extends JDOPersistenceManagerFactory {

  public DatastoreJDOPersistenceManagerFactory(Map props) {
    super(props);
  }

  protected JDOPersistenceManager newJDOPersistenceManager(
      JDOPersistenceManagerFactory jdoPersistenceManagerFactory, String userName, String password) {
    return new DatastoreJDOPersistenceManager(jdoPersistenceManagerFactory, userName, password);
  }

  /**
   * Return a new PersistenceManagerFactoryImpl with options set according to the given Properties.
   * Largely based on the parent class implementation of this method.
   *
   * @param overridingProps The Map of properties to initialize the PersistenceManagerFactory with.
   * @return A PersistenceManagerFactoryImpl with options set according to the given Properties.
   * @see JDOHelper#getPersistenceManagerFactory(Map)
   */
  public synchronized static PersistenceManagerFactory getPersistenceManagerFactory(
      Map overridingProps) {
    // Extract the properties into a Map allowing for a Properties object being used
    Map<String, Object> overridingMap;
    if (overridingProps instanceof Properties) {
      // Make sure we handle default properties too (SUN Properties class oddness)
      overridingMap = new HashMap<String, Object>();
      for (Enumeration e = ((Properties) overridingProps).propertyNames(); e.hasMoreElements();) {
        String param = (String) e.nextElement();
        overridingMap.put(param, ((Properties) overridingProps).getProperty(param));
      }
    } else {
      overridingMap = overridingProps;
    }

    if (overridingMap != null) {
      // This is an unfortunate way to do things, but I'm not aware of another
      // way to provide a default value for a specific persistence property
      // that already has a default value in the core plugin.xml.
      // plugin.xml in core assigns a default value of UPPERCASE for this
      // property, but we want a different default value for the app engine
      // pluging.  If I add this is a persistence property to our own
      // plugin.xml it doesn't always get honored because the plugin
      // persistence properties are not always read in the same order (there's
      // a Hashmap buried in there), and the default value is whichever one
      // gets read first.  So, in order to provide a plugin-specific default
      // value that conflicts with the default value for another plugin,
      // we set the property to the default value unless the user has
      // explicitly provided their own value in their config file.
      if (!overridingMap.containsKey("datanucleus.identifier.case")) {
        overridingMap.put("datanucleus.identifier.case", "PreserveCase");
      }
    }
    // Create the PMF and freeze it (JDO spec $11.7)
    final DatastoreJDOPersistenceManagerFactory pmf = new DatastoreJDOPersistenceManagerFactory(overridingMap);
    pmf.freezeConfiguration();

    return pmf;
  }

}
