// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine.jpa;

import org.datanucleus.jpa.EntityManagerFactoryImpl;
import org.datanucleus.jpa.PersistenceProviderImpl;
import org.datanucleus.store.appengine.Utils;

import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContextType;
import javax.persistence.spi.PersistenceUnitInfo;
import javax.jdo.PersistenceManagerFactory;

/**
 * @author Max Ross <maxr@google.com>
 */
public class DatastoreEntityManagerFactory extends EntityManagerFactoryImpl {

  public DatastoreEntityManagerFactory(String unitName, Map<String, Object> overridingProps) {
    super(unitName, manageOverridingProps(overridingProps));
  }

  public DatastoreEntityManagerFactory(PersistenceUnitInfo unitInfo, Map<String, Object> overridingProps) {
    super(unitInfo, manageOverridingProps(overridingProps));
  }

  private static Map<String, Object> manageOverridingProps(Map<String, Object> overridingProps) {
    if (overridingProps == null) {
      overridingProps = Utils.newHashMap();
    }
    // EntityManagerFactoryImpl, our parent class, will only accept
    // responsibility for a persistence unit if the persistence provider for
    // that unit is PersistenceProviderImpl (which it isn't - we've provided our
    // own PersistenceProvider impl), or if the "javax.persistence.provider"
    // option is set to the fqn of PersistenceProviderImpl.  We want our
    // parent class to accept responsibility for this persistence unit, so
    // we add this property with the expected value to the map if this
    // property isn't already set.  If it is already set then we're
    // not the right factory for this persistence unit anyway.
    if (!overridingProps.containsKey("javax.persistence.provider")) {
      overridingProps.put("javax.persistence.provider", PersistenceProviderImpl.class.getName());
    }
    // see DatastoreJDOPersistenceManagerFactory.getPersistenceManagerFactory
    // for an explanation of why we can't just add this as a persistence property
    // in plugin.xml
    if (!overridingProps.containsKey("datanucleus.identifier.case")) {
      overridingProps.put("datanucleus.identifier.case", "PreserveCase");
    }

    return overridingProps;
  }

  @Override
  protected EntityManager newEntityManager(PersistenceContextType contextType,
      PersistenceManagerFactory pmf) {
    return new DatastoreEntityManager(this, pmf, contextType);
  }
}
