/**********************************************************************
Copyright (c) 2011 Google Inc.

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

import org.datanucleus.util.NucleusLogger;

import com.google.appengine.datanucleus.test.Issue179Child;
import com.google.appengine.datanucleus.test.Issue179Parent;

public class Issue179Test extends JDOTestCase {

  public void testListRemoveByIndex() {
    Issue179Parent p = new Issue179Parent();
    p.setName("First Parent");
    Issue179Child c1 = new Issue179Child();
    c1.setName("Child 1");
    p.getChildren().add(c1);
    Issue179Child c2 = new Issue179Child();
    c2.setName("Child 2");
    p.getChildren().add(c2);
    Issue179Child c3 = new Issue179Child();
    c3.setName("Child 3");
    p.getChildren().add(c3);

    // Persist parent with 3 children
    Object id = null;
    beginTxn();
    pm.makePersistent(p);
    commitTxn();
    id = pm.getObjectId(p);
    pm.close();

    // Remove one-by-one in a transaction
    // This highlights a problem that once the first is deleted the datastore doesn't know about that deletion (til commit)
    pm = pmf.getPersistenceManager();
    NucleusLogger.GENERAL.debug(">> tx.begin");
    beginTxn();
    NucleusLogger.GENERAL.debug(">> pm.getObjectById(parent)");
    Issue179Parent p1 = (Issue179Parent)pm.getObjectById(id);
    NucleusLogger.GENERAL.debug(">> parent.children.remove(0)");
    p1.getChildren().remove(0);
    NucleusLogger.GENERAL.debug(">> parent.children.remove(0)");
    p1.getChildren().remove(0);
    NucleusLogger.GENERAL.debug(">> parent.children.remove(0)");
    p1.getChildren().remove(0);
    NucleusLogger.GENERAL.debug(">> tx.commit");
    commitTxn();
    pm.close();

    // The child list in the parent is actually correct here, just the query of the child objects with a parent is wrong
    pm = pmf.getPersistenceManager();
    beginTxn();
    Issue179Parent p2 = (Issue179Parent)pm.getObjectById(id);
    assertEquals(0, p2.getChildren().size());
    commitTxn();
  }
}
