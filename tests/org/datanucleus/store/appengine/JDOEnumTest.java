// Copyright 2009 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import org.datanucleus.test.HasEnumJDO;
import org.datanucleus.test.HasEnumJDO.MyEnum;
import static org.datanucleus.test.HasEnumJDO.MyEnum.*;

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
    assertNull(pojo.getMyEnumArray());
    assertNull(pojo.getMyEnumList());
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
