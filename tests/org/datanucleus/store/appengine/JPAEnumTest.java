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

import org.datanucleus.test.HasEnumJPA;
import org.datanucleus.test.HasEnumJPA.MyEnum;
import static org.datanucleus.test.HasEnumJPA.MyEnum.V1;
import static org.datanucleus.test.HasEnumJPA.MyEnum.V2;

import java.util.Arrays;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JPAEnumTest extends JPATestCase {

  public void testRoundtrip() {
    HasEnumJPA pojo = new HasEnumJPA();
    pojo.setMyEnum(V1);
    pojo.setMyEnumArray(new MyEnum[] {V2, V1, V2});
    pojo.setMyEnumList(Utils.newArrayList(V1, V2, V1));
    beginTxn();
    em.persist(pojo);
    commitTxn();

    beginTxn();
    pojo = em.find(HasEnumJPA.class, pojo.getKey());
    assertEquals(MyEnum.V1, pojo.getMyEnum());
    assertTrue(Arrays.equals(new MyEnum[] {V2, V1, V2 }, pojo.getMyEnumArray()));
    assertEquals(Utils.newArrayList(V1, V2, V1), pojo.getMyEnumList());
    commitTxn();
  }

  public void testRoundtrip_Null() {
    HasEnumJPA pojo = new HasEnumJPA();
    beginTxn();
    em.persist(pojo);
    commitTxn();

    beginTxn();
    pojo = em.find(HasEnumJPA.class, pojo.getKey());
    assertNull(pojo.getMyEnum());
    assertNull(pojo.getMyEnumArray());
    assertNull(pojo.getMyEnumList());
    commitTxn();
  }

  public void testRoundtrip_NullContainerVals() {
    HasEnumJPA pojo = new HasEnumJPA();
    pojo.setMyEnumArray(new MyEnum[] {null, V2});
    pojo.setMyEnumList(Utils.newArrayList(null, V2));
    beginTxn();
    em.persist(pojo);
    commitTxn();

    beginTxn();
    pojo = em.find(HasEnumJPA.class, pojo.getKey());
    assertNull(pojo.getMyEnum());
    assertTrue(Arrays.equals(new MyEnum[] {null, V2}, pojo.getMyEnumArray()));
    assertEquals(Utils.newArrayList(null, V2), pojo.getMyEnumList());
    commitTxn();
  }


}