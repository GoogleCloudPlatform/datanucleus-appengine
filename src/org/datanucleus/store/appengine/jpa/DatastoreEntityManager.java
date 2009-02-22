// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine.jpa;

import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.jpa.EntityManagerImpl;
import org.datanucleus.store.appengine.EntityUtils;
import org.datanucleus.store.appengine.jdo.DatastoreJDOPersistenceManager;

import javax.jdo.PersistenceManagerFactory;
import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceContextType;
import javax.persistence.PersistenceException;

/**
 * @author Max Ross <maxr@google.com>
 */
public class DatastoreEntityManager extends EntityManagerImpl {

  public DatastoreEntityManager(EntityManagerFactory emf, PersistenceManagerFactory pmf,
      PersistenceContextType contextType) {
    super(emf, pmf, contextType);
  }

  /**
   * @see DatastoreJDOPersistenceManager#getObjectById(Class, Object)
   */
  @Override
  public Object find(Class cls, Object key) {
    try {
      key = EntityUtils.idToInternalKey(getObjectManager(), cls, key);
    } catch (NucleusUserException e) {
      throw new PersistenceException(e);
    }
    return super.find(cls, key);
  }

}