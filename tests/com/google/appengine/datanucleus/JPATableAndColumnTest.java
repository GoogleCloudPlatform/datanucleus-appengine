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
package com.google.appengine.datanucleus;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.datanucleus.test.HasTableAndColumnsInMappingJPA;


/**
 * @author Max Ross <maxr@google.com>
 */
public class JPATableAndColumnTest extends JPATestCase {

  public void testInsert() throws EntityNotFoundException {
    HasTableAndColumnsInMappingJPA htacim = new HasTableAndColumnsInMappingJPA();
    htacim.setFoo("foo val");
    beginTxn();
    em.persist(htacim);
    commitTxn();
    assertNotNull(htacim.getId());
    Entity entity = ds.get(KeyFactory.createKey(
        HasTableAndColumnsInMappingJPA.TABLE_NAME, htacim.getId()));
    assertNotNull(entity);
    assertEquals(HasTableAndColumnsInMappingJPA.TABLE_NAME, entity.getKind());
    assertEquals("foo val", entity.getProperty(HasTableAndColumnsInMappingJPA.FOO_COLUMN_NAME));
  }

  public void testFetch() {
    Entity entity = new Entity(HasTableAndColumnsInMappingJPA.TABLE_NAME);
    entity.setProperty(HasTableAndColumnsInMappingJPA.FOO_COLUMN_NAME, "foo val");
    Key key = ds.put(entity);

    String keyStr = KeyFactory.keyToString(key);
    beginTxn();
    HasTableAndColumnsInMappingJPA htacim = em.find(HasTableAndColumnsInMappingJPA.class, keyStr);
    assertNotNull(htacim);
    assertEquals(Long.valueOf(key.getId()), htacim.getId());
    assertEquals("foo val", htacim.getFoo());
    commitTxn();
  }

}