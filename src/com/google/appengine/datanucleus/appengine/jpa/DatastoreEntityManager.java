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

import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.jpa.EntityManagerImpl;
import org.datanucleus.store.appengine.DatastoreManager;
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
    if (tx != null) {
      DatastoreManager storeMgr = (DatastoreManager) getObjectManager().getStoreManager();
      if (storeMgr.connectionFactoryIsTransactional()) {
        // install our own transaction object
        tx = new DatastoreEntityTransactionImpl(om);
      }
    }
  }

  /**
   * @see DatastoreJDOPersistenceManager#getObjectById(Class, Object)
   */
  @Override
  public Object find(Class cls, Object key) {
    try {
      key = EntityUtils.idToInternalKey(getObjectManager(), cls, key, false);
    } catch (NucleusUserException e) {
      throw new PersistenceException(e);
    }
    return super.find(cls, key);
  }

  @Override
  public void close() {
    DatastoreJPACallbackHandler.clearAttaching();
    super.close();
  }
}