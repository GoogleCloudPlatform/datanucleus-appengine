// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine.jpa;

import org.datanucleus.jpa.EntityManagerFactoryImpl;
import org.datanucleus.jpa.exceptions.NoPersistenceUnitException;
import org.datanucleus.jpa.exceptions.NoPersistenceXmlException;
import org.datanucleus.jpa.exceptions.NotProviderException;

import java.util.Map;

import javax.persistence.EntityManagerFactory;
import javax.persistence.spi.PersistenceProvider;
import javax.persistence.spi.PersistenceUnitInfo;

/**
 * @author Max Ross <maxr@google.com>
 */
public class DatastorePersistenceProvider implements PersistenceProvider {

  public EntityManagerFactory createEntityManagerFactory(String unitInfo, Map properties) {
    try {
      return new DatastoreEntityManagerFactory(unitInfo, properties);
    } catch (NotProviderException npe) {
    }
    // spec says to not let exceptions propagate, just return null
    return null;
  }

  public EntityManagerFactory createContainerEntityManagerFactory(
      PersistenceUnitInfo unitInfo, Map properties) {
    try {
      return new DatastoreEntityManagerFactory(unitInfo, properties);
    } catch (NotProviderException npe) {
    } catch (NoPersistenceUnitException npue) {
    } catch (NoPersistenceXmlException npxe) {
    }
    // spec says to not let exceptions propagate, just return null
    return null;
  }
}
