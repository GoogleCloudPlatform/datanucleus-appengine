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
    Map<String, Object> propsToReturn = Utils.newHashMap();
    if (overridingProps == null || !overridingProps.containsKey("javax.persistence.provider")) {
      propsToReturn.put("javax.persistence.provider", PersistenceProviderImpl.class.getName());
      if (overridingProps != null) {
        propsToReturn.putAll(overridingProps);
      }
    }
    return propsToReturn;
  }

  @Override
  protected EntityManager newEntityManager(PersistenceContextType contextType,
      PersistenceManagerFactory pmf) {
    return new DatastoreEntityManager(this, pmf, contextType);
  }
}
