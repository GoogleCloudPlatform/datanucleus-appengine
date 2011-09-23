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

import com.google.appengine.datanucleus.test.Issue138Child;
import com.google.appengine.datanucleus.test.Issue138Parent1;
import com.google.appengine.datanucleus.test.Issue138Parent2;

public class Issue138Test extends JDOTestCase {

  public void testMultipleOneToOne() {
    Issue138Parent2 p2 = new Issue138Parent2();
    Issue138Child c1 = new Issue138Child();
    c1.setName("c1");
    Issue138Child c2 = new Issue138Child();
    c2.setName("c2");
    p2.getChildren().add(c1);
    p2.getChildren().add(c1);
    pm.makePersistent(p2);

    Issue138Parent1 p1 = new Issue138Parent1();
    Issue138Child c3 = new Issue138Child();
    c1.setName("c3");
    p1.setChild(c3);
    pm.makePersistent(p1);
  }
}
