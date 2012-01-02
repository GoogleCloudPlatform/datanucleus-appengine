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

import com.google.appengine.datanucleus.test.Issue73Child;
import com.google.appengine.datanucleus.test.Issue73Parent;

public class Issue73Test extends JDOTestCase {

  public void testMultipleOneToOne() {
    Issue73Parent p = new Issue73Parent();
    p.setName("First Parent");
    Issue73Child c1 = new Issue73Child();
    c1.setName("Child 1");
    p.setChild1(c1);
    Issue73Child c2 = new Issue73Child();
    c2.setName("Child 2");
    p.setChild2(c2);

    // Persist parent with 2 children
    Object id = null;
    beginTxn();
    pm.makePersistent(p);
    commitTxn();
    id = pm.getObjectId(p);
    pm.close();

    // Retrieve and check the results
    pm = pmf.getPersistenceManager();
    beginTxn();
    Issue73Parent p1 = (Issue73Parent)pm.getObjectById(id);
    Issue73Child c1a = p1.getChild1();
    Issue73Child c2a = p1.getChild2();
    assertNotNull(c1a);
    assertNotNull(c2a);
    assertEquals("Child 1", c1a.getName());
    assertEquals("Child 2", c2a.getName());

    commitTxn();
  }
}
