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

import org.datanucleus.NucleusContext;
import org.datanucleus.api.jpa.JPAEntityManager;

import com.google.appengine.datanucleus.DatastoreManager;

import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceContextType;

/**
 * @author Max Ross <maxr@google.com>
 */
public class DatastoreEntityManager extends JPAEntityManager {

  public DatastoreEntityManager(EntityManagerFactory emf, NucleusContext nucCtx,
      PersistenceContextType contextType) {
    super(emf, nucCtx, contextType);
    if (tx != null) {
      DatastoreManager storeMgr = (DatastoreManager) getExecutionContext().getStoreManager();
      if (storeMgr.connectionFactoryIsTransactional()) {
        // install our own transaction object
        // TODO Remove this and just put it into ConnectionFactoryImpl like all other plugins
        tx = new DatastoreEntityTransactionImpl(om);
      }
    }
  }
}