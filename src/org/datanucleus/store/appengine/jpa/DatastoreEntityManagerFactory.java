/**********************************************************************
Copyright (c) 2009 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
**********************************************************************/
package org.datanucleus.store.appengine.jpa;

import org.datanucleus.jpa.EntityManagerFactoryImpl;
import org.datanucleus.jpa.PersistenceProviderImpl;
import org.datanucleus.store.appengine.Utils;

import java.util.Map;

import javax.jdo.PersistenceManagerFactory;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContextType;
import javax.persistence.spi.PersistenceUnitInfo;

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
    if (overridingProps != null) {
      propsToReturn.putAll(overridingProps);
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
    if (!propsToReturn.containsKey("javax.persistence.provider")) {
      propsToReturn.put("javax.persistence.provider", PersistenceProviderImpl.class.getName());
    }
    // see DatastoreJDOPersistenceManagerFactory.getPersistenceManagerFactory
    // for an explanation of why we can't just add this as a persistence property
    // in plugin.xml
    if (!propsToReturn.containsKey("datanucleus.identifier.case")) {
      propsToReturn.put("datanucleus.identifier.case", "PreserveCase");
    }

    return propsToReturn;
  }

  @Override
  protected EntityManager newEntityManager(PersistenceContextType contextType,
      PersistenceManagerFactory pmf) {
    return new DatastoreEntityManager(this, pmf, contextType);
  }
}
