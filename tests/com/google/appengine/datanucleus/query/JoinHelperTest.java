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
package com.google.appengine.datanucleus.query;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Link;
import com.google.appengine.datanucleus.JDOTestCase;
import com.google.appengine.datanucleus.Utils;


import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JoinHelperTest extends JDOTestCase {

  private static final String JOIN_PROP = "joinProp";

  private JoinHelper joinHelper;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    joinHelper = new JoinHelper();
  }

  public void testNoData() {
    assertNoResults(Collections.<Entity>emptyList());
    assertTrue(joinHelper.getMaterializedChildKeys().isEmpty());
  }

  public void testNoParentData() {
    Entity e = new Entity("Child", "k1");
    assertNoResults(Collections.<Entity>emptyList(), e);
    assertTrue(joinHelper.getMaterializedChildKeys().isEmpty());
  }

  public void testNoChildData() {
    Entity e = newParentEntity("k1", KeyFactory.createKey("Child", 10));
    assertNoResults(Collections.<Entity>singleton(e));
    assertTrue(joinHelper.getMaterializedChildKeys().isEmpty());
  }

  public void testParentHasNoJoinKeyProperty() {
    Entity e = new Entity("Parent", "k1");
    Entity c1 = new Entity("Child", "k0");
    Entity c2 = new Entity("Child", "k1");
    Entity c3 = new Entity("Child", "k2");
    assertNoResults(Collections.singleton(e), c1, c2, c3);
    assertTrue(joinHelper.getMaterializedChildKeys().isEmpty());
  }

  public void testParentHasNullJoinKeyProperty() {
    Entity e = new Entity("Parent", "k1");
    e.setProperty(JOIN_PROP, null);
    Entity c1 = new Entity("Child", "k0");
    Entity c2 = new Entity("Child", "k1");
    Entity c3 = new Entity("Child", "k2");
    assertNoResults(Collections.singleton(e), c1, c2, c3);
    assertTrue(joinHelper.getMaterializedChildKeys().isEmpty());
  }

  public void testParentHasNonKeyJoinKeyProperty() {
    Entity e = new Entity("Parent", "k1");
    e.setProperty(JOIN_PROP, "not a key");
    Entity c1 = new Entity("Child", "k0");
    Entity c2 = new Entity("Child", "k1");
    Entity c3 = new Entity("Child", "k2");
    assertNoResults(Collections.singleton(e), c1, c2, c3);
    assertTrue(joinHelper.getMaterializedChildKeys().isEmpty());
  }

  public void testParentHasNonKeyInJoinKeyList() {
    Entity e = newParentEntity("k1", 23, "not a key");
    Entity c1 = new Entity("Child", "k0");
    Entity c2 = new Entity("Child", "k1");
    Entity c3 = new Entity("Child", "k2");
    assertNoResults(Collections.singleton(e), c1, c2, c3);
    assertTrue(joinHelper.getMaterializedChildKeys().isEmpty());
  }

  public void testAllChildrenLargerThanAllJoinKeys() {
    Key key1 = KeyFactory.createKey("Child", "k0");
    Key key2 = KeyFactory.createKey("Child", "k1");
    Key key3 = KeyFactory.createKey("Child", "k2");
    Entity c1 = new Entity("Child", "k3");
    Entity p1 = newParentEntity("p1", key1);
    Entity p2 = newParentEntity("p2", key2);
    Entity p3 = newParentEntity("p3", key3);
    assertNoResults(Utils.newArrayList(p1, p2, p3), c1);
    assertEquals(Collections.singleton(c1.getKey()), joinHelper.getMaterializedChildKeys());
  }

  public void testAllChildrenSmallerThanAllJoinKeys() {
    Key key1 = KeyFactory.createKey("Child", "k1");
    Key key2 = KeyFactory.createKey("Child", "k2");
    Key key3 = KeyFactory.createKey("Child", "k3");
    Entity c1 = new Entity("Child", "k0");
    Entity p1 = newParentEntity("p1", key1);
    Entity p2 = newParentEntity("p2", key2);
    Entity p3 = newParentEntity("p3", key3);
    assertNoResults(Utils.newArrayList(p1, p2, p3), c1);
    assertEquals(Collections.singleton(c1.getKey()), joinHelper.getMaterializedChildKeys());
  }

  public void testOneMatch_OnlyJoinKey() {
    Key key1 = KeyFactory.createKey("Child", "k1");
    Key key2 = KeyFactory.createKey("Child", "k2");
    Key key3 = KeyFactory.createKey("Child", "k3");
    Entity c1 = new Entity("Child", "k1");
    Entity p1 = newParentEntity("p1", key1);
    Entity p2 = newParentEntity("p2", key2);
    Entity p3 = newParentEntity("p3", key3);
    assertEquals(Utils.newArrayList(p1), mergeJoin(Utils.newArrayList(p1, p2, p3), c1));
    assertEquals(Collections.singleton(c1.getKey()), joinHelper.getMaterializedChildKeys());
  }

  public void testOneMatch_FirstJoinKey() {
    Key key1 = KeyFactory.createKey("Child", "k1");
    Key key2 = KeyFactory.createKey("Child", "k2");
    Key key3 = KeyFactory.createKey("Child", "k3");
    Entity c1 = new Entity("Child", "k1");
    Entity p1 = newParentEntity("p1", key1, key2, key3);
    Entity p2 = newParentEntity("p2", key2);
    Entity p3 = newParentEntity("p3", key3);
    assertEquals(Utils.newArrayList(p1), mergeJoin(Utils.newArrayList(p1, p2, p3), c1));
    assertEquals(Collections.singleton(c1.getKey()), joinHelper.getMaterializedChildKeys());
  }

  public void testOneMatch_LastJoinKey() {
    Key key1 = KeyFactory.createKey("Child", "k1");
    Key key2 = KeyFactory.createKey("Child", "k2");
    Key key3 = KeyFactory.createKey("Child", "k3");
    Entity c1 = new Entity("Child", "k1");
    Entity p1 = newParentEntity("p1", key3, key2, key1);
    Entity p2 = newParentEntity("p2", key2);
    Entity p3 = newParentEntity("p3", key3);
    assertEquals(Utils.newArrayList(p1), mergeJoin(Utils.newArrayList(p1, p2, p3), c1));
    assertEquals(Collections.singleton(c1.getKey()), joinHelper.getMaterializedChildKeys());
  }

  public void testOneMatch_MultipleJoinKeys() {
    Key key1 = KeyFactory.createKey("Child", "k1");
    Key key2 = KeyFactory.createKey("Child", "k2");
    Key key3 = KeyFactory.createKey("Child", "k3");
    Entity c1 = new Entity("Child", "k1");
    Entity c2 = new Entity("Child", "k2");
    Entity p1 = newParentEntity("p1", key3, key2, key1);
    assertEquals(Utils.newArrayList(p1), mergeJoin(Utils.newArrayList(p1), c1, c2));
    assertEquals(Utils.newHashSet(c1.getKey(), c2.getKey()), joinHelper.getMaterializedChildKeys());
  }

  public void testOneMatch_SameJoinKeyMultipleTimes() {
    Key key1 = KeyFactory.createKey("Child", "k1");
    Key key2 = KeyFactory.createKey("Child", "k2");
    Key key3 = KeyFactory.createKey("Child", "k3");
    Entity c1 = new Entity(key1);
    Entity p1 = newParentEntity("p1", key1, key2, key1);
    Entity p2 = newParentEntity("p2", key2);
    Entity p3 = newParentEntity("p3", key3);
    assertEquals(Utils.newArrayList(p1), mergeJoin(Utils.newArrayList(p1, p2, p3), c1));
    assertEquals(Collections.singleton(c1.getKey()), joinHelper.getMaterializedChildKeys());
  }

  public void testComplicatedScenario() {
    List<Key> childKeys = Utils.newArrayList();
    List<Entity> childEntities = Utils.newArrayList();
    for (int i = 1; i <= 100; i++) {
      Key k = KeyFactory.createKey("Child", i);
      childKeys.add(k);
      childEntities.add(new Entity(k));
    }

    Entity p1 = newParentEntity("p1", childEntities.get(3).getKey());
    Entity p2 = newParentEntity(
        "p2",
        childEntities.get(3).getKey(),
        new Date(),
        childEntities.get(60).getKey(),
        childEntities.get(12).getKey());
    Entity p3 = newParentEntity(
        "p3",
        childEntities.get(18).getKey(),
        childEntities.get(19).getKey(),
        44L,
        childEntities.get(22).getKey());
    Entity p4 = newParentEntity(
        "p4",
        childEntities.get(38).getKey(),
        childEntities.get(89).getKey(),
        new Link("blar"),
        childEntities.get(60).getKey(), 
        childEntities.get(80).getKey());
    Entity p5 = newParentEntity(
        "p5",
        childEntities.get(49).getKey(),
        childEntities.get(65).getKey(),
        childEntities.get(99).getKey(),
        childEntities.get(78).getKey());
    Entity p6 = newParentEntity(
        "p6",
        childEntities.get(51).getKey(),
        childEntities.get(98).getKey(),
        childEntities.get(88).getKey(),
        childEntities.get(52).getKey());
    assertEquals(
        Utils.newArrayList(p1, p2, p3, p4, p5),
        mergeJoin(Utils.newArrayList(p1, p2, p3, p4, p5, p6), childEntities.subList(0, 50)));
    assertEquals(Utils.newHashSet(childKeys.subList(0, 50)), joinHelper.getMaterializedChildKeys());
  }

  public void testStreaming() {
    List<Key> childKeys = Utils.newArrayList();
    List<Entity> childEntities = Utils.newArrayList();
    List<Entity> parentEntities = Utils.newArrayList();
    for (int i = 1; i <= 100; i++) {
      Key k = KeyFactory.createKey("Child", i);
      childKeys.add(k);
      childEntities.add(new Entity(k));
      parentEntities.add(newParentEntity("p" + i, k));
    }
    int i = 0;
    for (Entity e : mergeJoin(parentEntities, childEntities)) {
      assertEquals(parentEntities.get(i++), e);
      assertEquals(Utils.newHashSet(childKeys.subList(0, i)), joinHelper.getMaterializedChildKeys());
    }
  }

  private static Entity newParentEntity(String name, Object... joinKeys) {
    Entity e = new Entity("parent", name);
    Object val = joinKeys.length == 1 ? joinKeys[0] : Arrays.asList(joinKeys);
    e.setProperty(JOIN_PROP, val);
    return e;
  }

  private Iterable<Entity> mergeJoin(Iterable<Entity> parents, Entity... kids) {
    return mergeJoin(parents, Arrays.asList(kids));
  }

  private Iterable<Entity> mergeJoin(Iterable<Entity> parents, Iterable<Entity> kids) {
    return joinHelper.mergeJoin(JOIN_PROP, parents, kids.iterator());
  }

  private void assertNoResults(Iterable<Entity> parents, Entity... kids) {
    assertFalse(mergeJoin(parents, kids).iterator().hasNext());
  }
}
