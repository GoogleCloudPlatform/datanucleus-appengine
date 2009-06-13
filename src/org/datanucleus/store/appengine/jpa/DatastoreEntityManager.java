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

  /**
   * Used to provide access to the entity being merged.  See the comment on
   * {@link #merge(Object)} for more information.
   */
  private static final ThreadLocal<Object> MERGE_ENTITY = new ThreadLocal<Object>();

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

  /**
   * Sigh.  When you have a unidirectional OneToMany on a parent object
   * and you add a new child to a detached instance of the parent,
   * DataNucleus first adds the child without the parent and then
   * issues an update to add the parent key.  This doesn't work for App Engine
   * since we can only handle one write per entity.  We need to know
   * what the object's parent is when the object is first inserted.
   * In order to provide this information we set the Entity being merged
   * (the parent) in a ThreadLocal so that we can pull it out later.
   * This doesn't work when you have children being
   * added multiple levels down, but it's a temporary fix that should
   * hold until we can support multiple writes.  Users will get a "parent switch"
   * exception in this scenario so even though it's broken, it doesn't result
   * in data corruption.
   */
  @Override
  public Object merge(Object entity) {
    MERGE_ENTITY.set(entity);
    try {
      return super.merge(entity);
    } finally {
      MERGE_ENTITY.remove();
    }
  }

  public static Object getMergingEntity() {
    return MERGE_ENTITY.get();
  }
}