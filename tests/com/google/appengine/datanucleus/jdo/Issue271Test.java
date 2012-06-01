/**********************************************************************
Copyright (c) 2012 Google Inc.

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
package com.google.appengine.datanucleus.jdo;

import java.util.List;

import javax.jdo.JDOHelper;

import org.datanucleus.util.NucleusLogger;

import com.google.appengine.datanucleus.test.jdo.Issue271Child;
import com.google.appengine.datanucleus.test.jdo.Issue271Parent;

public class Issue271Test extends JDOTestCase {

  public void testPersistThenDeleteElement() {
    Object pId = null;
    Object c1Id = null;
    Object c2Id = null;
    try {
      pm.currentTransaction().begin();
      Issue271Parent p = new Issue271Parent();
      Issue271Child c1 = new Issue271Child();
      Issue271Child c2 = new Issue271Child();
      p.getChildren().add(c1);
      p.getChildren().add(c2);
      c1.setParent(p);
      c2.setParent(p);
      pm.makePersistent(p);
      pm.currentTransaction().commit();

      pId = pm.getObjectId(p);
      c1Id = pm.getObjectId(c1);
      c2Id = pm.getObjectId(c2);
    } catch (Exception e) {
      NucleusLogger.GENERAL.error(">> Exception in persist", e);
      fail("Failure during persist : " + e.getMessage());
    } finally {
      if (pm.currentTransaction().isActive()) {
        pm.currentTransaction().rollback();
      }
      pm.close();
    }
    pmf.getDataStoreCache().evictAll();

    pm = pmf.getPersistenceManager();
    Issue271Child c1 = (Issue271Child) pm.getObjectById(c1Id);
    pm.deletePersistent(c1);
    pm.close();

    pm = pmf.getPersistenceManager();
    Issue271Parent p = (Issue271Parent)pm.getObjectById(pId);
    List<Issue271Child> children = p.getChildren();
    assertNotNull(children);
    NucleusLogger.GENERAL.info(">> Accessing children");
    Issue271Child c = children.get(0);
    assertEquals(1, children.size());
    assertEquals(c2Id, JDOHelper.getObjectId(c));
  }
}
