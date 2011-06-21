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

import com.google.appengine.datanucleus.test.HasEnumJDO;
import com.google.appengine.datanucleus.test.HasEnumJDO.MyEnum;

import static com.google.appengine.datanucleus.test.HasEnumJDO.MyEnum.V1;
import static com.google.appengine.datanucleus.test.HasEnumJDO.MyEnum.V2;

import java.util.Arrays;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JDOEnumTest extends JDOTestCase {

  public void testRoundtrip() {
    HasEnumJDO pojo = new HasEnumJDO();
    pojo.setMyEnum(V1);
    pojo.setMyEnumArray(new MyEnum[] {V2, V1, V2});
    pojo.setMyEnumList(Utils.newArrayList(V1, V2, V1));
    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();

    beginTxn();
    pojo = pm.getObjectById(HasEnumJDO.class, pojo.getKey());
    assertEquals(HasEnumJDO.MyEnum.V1, pojo.getMyEnum());
    assertTrue(Arrays.equals(new MyEnum[] {V2, V1, V2 }, pojo.getMyEnumArray()));
    assertEquals(Utils.newArrayList(V1, V2, V1), pojo.getMyEnumList());
    commitTxn();
  }

  public void testRoundtrip_Null() {
    HasEnumJDO pojo = new HasEnumJDO();
    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();

    beginTxn();
    pojo = pm.getObjectById(HasEnumJDO.class, pojo.getKey());
    assertNull(pojo.getMyEnum());
    assertNotNull(pojo.getMyEnumArray());
    assertEquals(0, pojo.getMyEnumArray().length);
    assertNotNull(pojo.getMyEnumList());
    assertTrue(pojo.getMyEnumList().isEmpty());
    commitTxn();
  }

  public void testRoundtrip_NullContainerVals() {
    HasEnumJDO pojo = new HasEnumJDO();
    pojo.setMyEnumArray(new MyEnum[] {null, V2});
    pojo.setMyEnumList(Utils.newArrayList(null, V2));
    beginTxn();
    pm.makePersistent(pojo);
    commitTxn();

    beginTxn();
    pojo = pm.getObjectById(HasEnumJDO.class, pojo.getKey());
    assertNull(pojo.getMyEnum());
    assertTrue(Arrays.equals(new MyEnum[] {null, V2}, pojo.getMyEnumArray()));
    assertEquals(Utils.newArrayList(null, V2), pojo.getMyEnumList());
    commitTxn();
  }


}
