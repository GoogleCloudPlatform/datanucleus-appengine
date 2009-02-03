// Copyright 2009 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import org.datanucleus.test.HasEnumJPA;
import org.datanucleus.test.HasEnumJPA.MyEnum;
import static org.datanucleus.test.HasEnumJPA.MyEnum.*;

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