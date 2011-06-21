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
package com.google.appengine.datanucleus.jpa;

import org.datanucleus.jpa.EntityManagerFactoryImpl;
import org.datanucleus.jpa.PersistenceProviderImpl;

import com.google.appengine.datanucleus.ConcurrentHashMapHelper;
import com.google.appengine.datanucleus.Utils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jdo.PersistenceManagerFactory;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceContextType;
import javax.persistence.spi.PersistenceUnitInfo;

/**
 * @author Max Ross <maxr@google.com>
 */
public class DatastoreEntityManagerFactory extends EntityManagerFactoryImpl {

  /**
   * Keeps track of the number of instances we've allocated per EMF in this
   * class loader.  We do this to try and detect when users are allocating
   * these over and over when they should just be allocating one and reusing
   * it.
   */
  private static final ConcurrentHashMap<String, AtomicInteger> NUM_INSTANCES_PER_PERSISTENCE_UNIT =
      new ConcurrentHashMap<String, AtomicInteger>();

  /**
   * System property that enables users to disable the emf warning.  Useful for situations
   * where you really do want to create the same EMF over and over, like unit tests.
   */
  public static final String
      DISABLE_DUPLICATE_EMF_EXCEPTION_PROPERTY = "appengine.orm.disable.duplicate.emf.exception";

  private static final String DUPLICATE_EMF_ERROR_FORMAT =
      "Application code attempted to create a EntityManagerFactory named %s, but "
      + "one with this name already exists!  Instances of EntityManagerFactory are extremely slow "
      + "to create and it is usually not necessary to create one with a given name more than once.  "
      + "Instead, create a singleton and share it throughout your code.  If you really do need "
      + "to create a duplicate EntityManagerFactory (such as for a unittest suite), set the "
      + DISABLE_DUPLICATE_EMF_EXCEPTION_PROPERTY + " system property to avoid this error.";

  public DatastoreEntityManagerFactory(String unitName, Map<String, Object> overridingProps) {
    super(unitName, manageOverridingProps(overridingProps));
    checkForRepeatedAllocation(unitName);
  }

  public DatastoreEntityManagerFactory(PersistenceUnitInfo unitInfo, Map<String, Object> overridingProps) {
    super(unitInfo, manageOverridingProps(overridingProps));
    checkForRepeatedAllocation(unitInfo.getPersistenceUnitName());
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

  /**
   * @return {@code true} if the user has already allocated a
   * {@link PersistenceManagerFactory} with the provided name, {@code false}
   * otherwise.
   */
  private static boolean alreadyAllocated(String name) {
    // It's not clear if it is possible create an EMF without a name,
    // but since we do our duplicate detection based on name we need to be
    // careful.  We have to short-circuit here because ConcurrentHashMap
    // throws NPE if you pass it a null key.
    if (name == null) {
      return false;
    }
    AtomicInteger count =
        ConcurrentHashMapHelper.getCounter(NUM_INSTANCES_PER_PERSISTENCE_UNIT, name);
    return count.incrementAndGet() > 1 &&
        !System.getProperties().containsKey(DISABLE_DUPLICATE_EMF_EXCEPTION_PROPERTY);
  }

  /**
   * Throws {@link IllegalStateException} when the user allocates more than one
   * {@link EntityManagerFactory} with the same name, unless the user has
   * added the {@link #DISABLE_DUPLICATE_EMF_EXCEPTION_PROPERTY} system property.
   */
  private void checkForRepeatedAllocation(String name) {
    if (alreadyAllocated(name)) {
      try {
        close();
      } finally {
        // this exception is more important than any exception that might be
        // raised by close()
        throw new IllegalStateException(String.format(DUPLICATE_EMF_ERROR_FORMAT, name));
      }
    }
  }

  public PersistenceManagerFactory getPersistenceManagerFactory() {
    return pmf;
  }

  // visible for testing
  static ConcurrentHashMap<String, AtomicInteger> getNumInstancesPerPersistenceUnit() {
    return NUM_INSTANCES_PER_PERSISTENCE_UNIT;
  }
}
