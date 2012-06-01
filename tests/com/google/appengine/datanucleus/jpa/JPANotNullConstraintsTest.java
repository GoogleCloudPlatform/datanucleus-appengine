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
package com.google.appengine.datanucleus.jpa;

import com.google.appengine.datanucleus.test.jpa.HasNotNullConstraintsJPA;

import javax.persistence.PersistenceException;

public class JPANotNullConstraintsTest extends JPATestCase {

  private static final Boolean VAL_BOOL = Boolean.TRUE;
  private static final Character VAL_CHAR = 'c';
  private static final Byte VAL_BYTE = 0x1;
  private static final Short VAL_SHORT = (short) 1;
  private static final Integer VAL_INT = 2;
  private static final Long VAL_LONG = 3L;
  private static final Float VAL_FLOAT = 4f;
  private static final Double VAL_DOUBLE = 5d;
  private static final String VAL_STRING = "yam";

  public void testInsertNotNull() {
    HasNotNullConstraintsJPA obj = create();

    obj = em.find(HasNotNullConstraintsJPA.class, obj.getId());
    assertEquals(VAL_BOOL, obj.getBool());
    assertEquals(VAL_CHAR, obj.getC());
    assertEquals(VAL_BYTE, obj.getB());
    assertEquals(VAL_SHORT, obj.getS());
    assertEquals(VAL_INT, obj.getI());
    assertEquals(VAL_LONG, obj.getL());
    assertEquals(VAL_FLOAT, obj.getF());
    assertEquals(VAL_DOUBLE, obj.getD());
    assertEquals(VAL_STRING, obj.getStr());
  }

  public void testInsertNull() {
    try {
      beginTxn();
      em.persist(createHasNotNullConstraintsJPA(null, VAL_CHAR, VAL_BYTE,
                                                VAL_SHORT, VAL_INT, VAL_LONG, VAL_FLOAT, VAL_DOUBLE,
                                                VAL_STRING));
      commitTxn();
      fail("expected Exception");
    } catch (PersistenceException e) {
      // good
      assertEquals(0, countForClass(HasNotNullConstraintsJPA.class));
    }
    try {
      beginTxn();
      em.persist(createHasNotNullConstraintsJPA(VAL_BOOL, null, VAL_BYTE,
                                                VAL_SHORT, VAL_INT, VAL_LONG, VAL_FLOAT, VAL_DOUBLE,
                                                VAL_STRING));
      commitTxn();
      fail("expected Exception");
    } catch (PersistenceException e) {
      // good
      assertEquals(0, countForClass(HasNotNullConstraintsJPA.class));
    }
    try {
      beginTxn();
      em.persist(createHasNotNullConstraintsJPA(VAL_BOOL, VAL_CHAR, null,
                                                VAL_SHORT, VAL_INT, VAL_LONG, VAL_FLOAT, VAL_DOUBLE,
                                                VAL_STRING));
      commitTxn();
      fail("expected Exception");
    } catch (PersistenceException e) {
      // good
      assertEquals(0, countForClass(HasNotNullConstraintsJPA.class));
    }
    try {
      beginTxn();
      em.persist(createHasNotNullConstraintsJPA(VAL_BOOL, VAL_CHAR, VAL_BYTE,
                                                null, VAL_INT, VAL_LONG, VAL_FLOAT, VAL_DOUBLE,
                                                VAL_STRING));
      commitTxn();
      fail("expected Exception");
    } catch (PersistenceException e) {
      // good
      assertEquals(0, countForClass(HasNotNullConstraintsJPA.class));
    }
    try {
      beginTxn();
      em.persist(createHasNotNullConstraintsJPA(VAL_BOOL, VAL_CHAR, VAL_BYTE,
                                                VAL_SHORT, null, VAL_LONG, VAL_FLOAT, VAL_DOUBLE,
                                                VAL_STRING));
      commitTxn();
      fail("expected Exception");
    } catch (PersistenceException e) {
      // good
      assertEquals(0, countForClass(HasNotNullConstraintsJPA.class));
    }
    try {
      beginTxn();
      em.persist(createHasNotNullConstraintsJPA(null, VAL_CHAR, VAL_BYTE,
                                                VAL_SHORT, VAL_INT, null, VAL_FLOAT, VAL_DOUBLE,
                                                VAL_STRING));
      commitTxn();
      fail("expected Exception");
    } catch (PersistenceException e) {
      // good
      assertEquals(0, countForClass(HasNotNullConstraintsJPA.class));
    }
    try {
      beginTxn();
      em.persist(createHasNotNullConstraintsJPA(null, VAL_CHAR, VAL_BYTE,
                                                VAL_SHORT, VAL_INT, VAL_LONG, null, VAL_DOUBLE,
                                                VAL_STRING));
      commitTxn();
      fail("expected Exception");
    } catch (PersistenceException e) {
      // good
      assertEquals(0, countForClass(HasNotNullConstraintsJPA.class));
    }
    try {
      beginTxn();
      em.persist(createHasNotNullConstraintsJPA(null, VAL_CHAR, VAL_BYTE,
                                                VAL_SHORT, VAL_INT, VAL_LONG, VAL_FLOAT, null,
                                                VAL_STRING));
      commitTxn();
      fail("expected Exception");
    } catch (PersistenceException e) {
      // good
      assertEquals(0, countForClass(HasNotNullConstraintsJPA.class));
    }
    try {
      beginTxn();
      em.persist(createHasNotNullConstraintsJPA(null, VAL_CHAR, VAL_BYTE,
                                                VAL_SHORT, VAL_INT, VAL_LONG, VAL_FLOAT, VAL_DOUBLE,
                                                null));
      commitTxn();
      fail("expected Exception");
    } catch (PersistenceException e) {
      // good
      assertEquals(0, countForClass(HasNotNullConstraintsJPA.class));
    }
  }

  public void testUpdateNotNull() {
    HasNotNullConstraintsJPA obj = create();

    beginTxn();
    obj = em.find(HasNotNullConstraintsJPA.class, obj.getId());
    assertTrue(obj.getBool());
    obj.setBool(false);
    commitTxn();

    obj = em.find(HasNotNullConstraintsJPA.class, obj.getId());
    assertFalse(obj.getBool());
    assertEquals(VAL_CHAR, obj.getC());
    assertEquals(VAL_BYTE, obj.getB());
    assertEquals(VAL_SHORT, obj.getS());
    assertEquals(VAL_INT, obj.getI());
    assertEquals(VAL_LONG, obj.getL());
    assertEquals(VAL_FLOAT, obj.getF());
    assertEquals(VAL_DOUBLE, obj.getD());
    assertEquals(VAL_STRING, obj.getStr());
  }

  public void testUpdateNull() {
    HasNotNullConstraintsJPA obj = create();

    doUpdate(obj.getId(), new Update() {
      public void update(HasNotNullConstraintsJPA obj) {
        obj.setBool(null);
      }
    });
    doUpdate(obj.getId(), new Update() {
      public void update(HasNotNullConstraintsJPA obj) {
        obj.setC(null);
      }
    });
    doUpdate(obj.getId(), new Update() {
      public void update(HasNotNullConstraintsJPA obj) {
        obj.setB(null);
      }
    });
    doUpdate(obj.getId(), new Update() {
      public void update(HasNotNullConstraintsJPA obj) {
        obj.setS(null);
      }
    });
    doUpdate(obj.getId(), new Update() {
      public void update(HasNotNullConstraintsJPA obj) {
        obj.setI(null);
      }
    });
    doUpdate(obj.getId(), new Update() {
      public void update(HasNotNullConstraintsJPA obj) {
        obj.setL(null);
      }
    });
    doUpdate(obj.getId(), new Update() {
      public void update(HasNotNullConstraintsJPA obj) {
        obj.setF(null);
      }
    });
    doUpdate(obj.getId(), new Update() {
      public void update(HasNotNullConstraintsJPA obj) {
        obj.setStr(null);
      }
    });
  }

  public void testDeleteNull() {
    doRemove(create().getId(), new Update() {
      public void update(HasNotNullConstraintsJPA obj) {
        obj.setBool(null);
      }
    });
    doRemove(create().getId(), new Update() {
      public void update(HasNotNullConstraintsJPA obj) {
        obj.setC(null);
      }
    });
    doRemove(create().getId(), new Update() {
      public void update(HasNotNullConstraintsJPA obj) {
        obj.setB(null);
      }
    });
    doRemove(create().getId(), new Update() {
      public void update(HasNotNullConstraintsJPA obj) {
        obj.setS(null);
      }
    });
    doRemove(create().getId(), new Update() {
      public void update(HasNotNullConstraintsJPA obj) {
        obj.setI(null);
      }
    });
    doRemove(create().getId(), new Update() {
      public void update(HasNotNullConstraintsJPA obj) {
        obj.setL(null);
      }
    });
    doRemove(create().getId(), new Update() {
      public void update(HasNotNullConstraintsJPA obj) {
        obj.setF(null);
      }
    });
    doRemove(create().getId(), new Update() {
      public void update(HasNotNullConstraintsJPA obj) {
        obj.setStr(null);
      }
    });
  }

  private HasNotNullConstraintsJPA create() {
    beginTxn();
    HasNotNullConstraintsJPA obj = createHasNotNullConstraintsJPA(VAL_BOOL, VAL_CHAR,
                                                                  VAL_BYTE, VAL_SHORT, VAL_INT,
                                                                  VAL_LONG, VAL_FLOAT, VAL_DOUBLE,
                                                                  VAL_STRING);
    em.persist(obj);
    commitTxn();
    return obj;
  }

  private void doUpdate(Long id, Update update) {
    HasNotNullConstraintsJPA obj;
    beginTxn();
    try {
      obj = em.find(HasNotNullConstraintsJPA.class, id);
      update.update(obj);
      em.persist(obj);
      commitTxn();
      fail("expected Exception");
    } catch (PersistenceException e) {
      // good
    }

    //	make sure no changes made 
    beginTxn();
    obj = em.find(HasNotNullConstraintsJPA.class, id);
    rollbackTxn();
    assertEquals(VAL_BOOL, obj.getBool());
    assertEquals(VAL_CHAR, obj.getC());
    assertEquals(VAL_BYTE, obj.getB());
    assertEquals(VAL_SHORT, obj.getS());
    assertEquals(VAL_INT, obj.getI());
    assertEquals(VAL_LONG, obj.getL());
    assertEquals(VAL_FLOAT, obj.getF());
    assertEquals(VAL_DOUBLE, obj.getD());
    assertEquals(VAL_STRING, obj.getStr());
  }

  private void doRemove(Long id, Update update) {
    beginTxn();
    HasNotNullConstraintsJPA obj = em.find(HasNotNullConstraintsJPA.class, id);
    update.update(obj);
    em.remove(obj);
    commitTxn();

    assertEquals(0, countForClass(HasNotNullConstraintsJPA.class));
  }

  private HasNotNullConstraintsJPA createHasNotNullConstraintsJPA(Boolean bool, Character c, Byte b,
                                                                  Short s, Integer i, Long l,
                                                                  Float f, Double d, String str) {
    HasNotNullConstraintsJPA pc = new HasNotNullConstraintsJPA();
    pc.setBool(bool);
    pc.setC(c);
    pc.setB(b);
    pc.setS(s);
    pc.setI(i);
    pc.setL(l);
    pc.setF(f);
    pc.setD(d);
    pc.setStr(str);
    return pc;
  }

  interface Update {

    void update(HasNotNullConstraintsJPA obj);
  }
}
