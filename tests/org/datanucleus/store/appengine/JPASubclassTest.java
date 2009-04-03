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
package org.datanucleus.store.appengine;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.KeyFactory;

import org.datanucleus.test.HasSubclassJPA;
import org.datanucleus.test.IsSubclassJPA;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JPASubclassTest extends JPATestCase {

  public void testInsertParent_KeyOnParent() throws EntityNotFoundException {
    HasSubclassJPA pojo = new HasSubclassJPA();
    pojo.setParentString("yar");
    beginTxn();
    em.persist(pojo);
    commitTxn();

    Entity e = ldth.ds.get(KeyFactory.createKey(HasSubclassJPA.class.getSimpleName(), pojo.getId()));
    assertEquals("yar", e.getProperty("parentString"));
  }

  public void testInsertChild_KeyOnParent() throws EntityNotFoundException {
    IsSubclassJPA pojo = new IsSubclassJPA();
    pojo.setParentString("yar");
    pojo.setChildString("childyar");
    beginTxn();
    em.persist(pojo);
    commitTxn();

    Entity e = ldth.ds.get(KeyFactory.createKey(IsSubclassJPA.class.getSimpleName(), pojo.getId()));
    assertEquals("yar", e.getProperty("parentString"));
    assertEquals("childyar", e.getProperty("childString"));

  }

  public void testFetchParent_KeyOnParent() {
    Entity e = new Entity(HasSubclassJPA.class.getSimpleName());
    e.setProperty("parentString", "yar");
    ldth.ds.put(e);

    beginTxn();
    HasSubclassJPA pojo = em.find(HasSubclassJPA.class, e.getKey());
    assertEquals("yar", pojo.getParentString());
  }

  public void testFetchChild_KeyOnParent() {
    Entity e = new Entity(IsSubclassJPA.class.getSimpleName());
    e.setProperty("parentString", "yar");
    e.setProperty("childString", "childyar");
    ldth.ds.put(e);

    beginTxn();
    IsSubclassJPA pojo = em.find(IsSubclassJPA.class, e.getKey());
    assertEquals("yar", pojo.getParentString());
    assertEquals("childyar", pojo.getChildString());
  }
}