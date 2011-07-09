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

import com.google.appengine.datanucleus.test.HasNotNullConstraintsJDO;

import javax.jdo.JDOUserException;

public class JDONotNullConstraintsTest extends JDOTestCase {

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
    HasNotNullConstraintsJDO pc = createHasConstraintsJDO(VAL_BOOL, VAL_CHAR,
                                                          VAL_BYTE, VAL_SHORT, VAL_INT, VAL_LONG,
                                                          VAL_FLOAT, VAL_DOUBLE, VAL_STRING);
    makePersistentInTxn(pc, TXN_START_END);

    beginTxn();
    pc = pm.getObjectById(HasNotNullConstraintsJDO.class, pc.getId());
    assertEquals(VAL_BOOL, pc.getBool());
    assertEquals(VAL_CHAR, pc.getC());
    assertEquals(VAL_BYTE, pc.getB());
    assertEquals(VAL_SHORT, pc.getS());
    assertEquals(VAL_INT, pc.getI());
    assertEquals(VAL_LONG, pc.getL());
    assertEquals(VAL_FLOAT, pc.getF());
    assertEquals(VAL_DOUBLE, pc.getD());
    assertEquals(VAL_STRING, pc.getStr());
    commitTxn();
  }

  public void testInsertNull() {
    try {
      makePersistentInTxn(createHasConstraintsJDO(null, 'c', (byte) 0x1,
                                                  (short) 1, 2, 3L, 4f, 5d, "yam"), TXN_START_END);
      fail("expected Exception");
    } catch (JDOUserException e) {
      // good
      assertEquals(0, countForClass(HasNotNullConstraintsJDO.class));
    }
    try {
      makePersistentInTxn(createHasConstraintsJDO(true, null, (byte) 0x1,
                                                  (short) 1, 2, 3L, 4f, 5d, "yam"), TXN_START_END);
      fail("expected Exception");
    } catch (JDOUserException e) {
      // good
      assertEquals(0, countForClass(HasNotNullConstraintsJDO.class));
    }
    try {
      makePersistentInTxn(createHasConstraintsJDO(true, 'c', null, (short) 1,
                                                  2, 3L, 4f, 5d, "yam"), TXN_START_END);
      fail("expected Exception");
    } catch (JDOUserException e) {
      // good
      assertEquals(0, countForClass(HasNotNullConstraintsJDO.class));
    }
    try {
      makePersistentInTxn(createHasConstraintsJDO(true, 'c', (byte) 0x1, null,
                                                  2, 3L, 4f, 5d, "yam"), TXN_START_END);
      fail("expected Exception");
    } catch (JDOUserException e) {
      // good
      assertEquals(0, countForClass(HasNotNullConstraintsJDO.class));
    }
    try {
      makePersistentInTxn(createHasConstraintsJDO(true, 'c', (byte) 0x1,
                                                  (short) 1, null, 3L, 4f, 5d, "yam"),
                          TXN_START_END);
      fail("expected Exception");
    } catch (JDOUserException e) {
      // good
      assertEquals(0, countForClass(HasNotNullConstraintsJDO.class));
    }
    try {
      makePersistentInTxn(createHasConstraintsJDO(true, 'c', (byte) 0x1,
                                                  (short) 1, 2, null, 4f, 5d, "yam"),
                          TXN_START_END);
      fail("expected Exception");
    } catch (JDOUserException e) {
      // good
      assertEquals(0, countForClass(HasNotNullConstraintsJDO.class));
    }
    try {
      makePersistentInTxn(createHasConstraintsJDO(true, 'c', (byte) 0x1,
                                                  (short) 1, 2, 3L, null, 5d, "yam"),
                          TXN_START_END);
      fail("expected Exception");
    } catch (JDOUserException e) {
      // good
      assertEquals(0, countForClass(HasNotNullConstraintsJDO.class));
    }
    try {
      makePersistentInTxn(createHasConstraintsJDO(true, 'c', (byte) 0x1,
                                                  (short) 1, 2, 3L, 4f, null, "yam"),
                          TXN_START_END);
      fail("expected Exception");
    } catch (JDOUserException e) {
      // good
      assertEquals(0, countForClass(HasNotNullConstraintsJDO.class));
    }
    try {
      makePersistentInTxn(createHasConstraintsJDO(true, 'c', (byte) 0x1,
                                                  (short) 1, 2, 3L, 4f, 5d, null), TXN_START_END);
      fail("expected Exception");
    } catch (JDOUserException e) {
      // good
      assertEquals(0, countForClass(HasNotNullConstraintsJDO.class));
    }
  }

  public void testUpdateNotNull() {
    HasNotNullConstraintsJDO obj = create();

    beginTxn();
    obj = pm.getObjectById(HasNotNullConstraintsJDO.class, obj.getId());
    assertTrue(obj.getBool());
    obj.setBool(false);
    commitTxn();

    beginTxn();
    obj = pm.getObjectById(HasNotNullConstraintsJDO.class, obj.getId());
    assertFalse(obj.getBool());
    assertEquals(VAL_CHAR, obj.getC());
    assertEquals(VAL_BYTE, obj.getB());
    assertEquals(VAL_SHORT, obj.getS());
    assertEquals(VAL_INT, obj.getI());
    assertEquals(VAL_LONG, obj.getL());
    assertEquals(VAL_FLOAT, obj.getF());
    assertEquals(VAL_DOUBLE, obj.getD());
    assertEquals(VAL_STRING, obj.getStr());
    commitTxn();
  }

  public void testUpdateNull() {
    HasNotNullConstraintsJDO pc = create();

    doUpdate(pc.getId(), new Update() {
      public void update(HasNotNullConstraintsJDO pc) {
        pc.setBool(null);
      }
    });
    doUpdate(pc.getId(), new Update() {
      public void update(HasNotNullConstraintsJDO pc) {
        pc.setC(null);
      }
    });
    doUpdate(pc.getId(), new Update() {
      public void update(HasNotNullConstraintsJDO pc) {
        pc.setB(null);
      }
    });
    doUpdate(pc.getId(), new Update() {
      public void update(HasNotNullConstraintsJDO pc) {
        pc.setS(null);
      }
    });
    doUpdate(pc.getId(), new Update() {
      public void update(HasNotNullConstraintsJDO pc) {
        pc.setI(null);
      }
    });
    doUpdate(pc.getId(), new Update() {
      public void update(HasNotNullConstraintsJDO pc) {
        pc.setL(null);
      }
    });
    doUpdate(pc.getId(), new Update() {
      public void update(HasNotNullConstraintsJDO pc) {
        pc.setF(null);
      }
    });
    doUpdate(pc.getId(), new Update() {
      public void update(HasNotNullConstraintsJDO pc) {
        pc.setD(null);
      }
    });
    doUpdate(pc.getId(), new Update() {
      public void update(HasNotNullConstraintsJDO pc) {
        pc.setStr(null);
      }
    });
  }

  public void testDeleteNull() {
    doRemove(create().getId(), new Update() {
      public void update(HasNotNullConstraintsJDO obj) {
        obj.setBool(null);
      }
    });
    doRemove(create().getId(), new Update() {
      public void update(HasNotNullConstraintsJDO obj) {
        obj.setC(null);
      }
    });
    doRemove(create().getId(), new Update() {
      public void update(HasNotNullConstraintsJDO obj) {
        obj.setB(null);
      }
    });
    doRemove(create().getId(), new Update() {
      public void update(HasNotNullConstraintsJDO obj) {
        obj.setS(null);
      }
    });
    doRemove(create().getId(), new Update() {
      public void update(HasNotNullConstraintsJDO obj) {
        obj.setI(null);
      }
    });
    doRemove(create().getId(), new Update() {
      public void update(HasNotNullConstraintsJDO obj) {
        obj.setL(null);
      }
    });
    doRemove(create().getId(), new Update() {
      public void update(HasNotNullConstraintsJDO obj) {
        obj.setF(null);
      }
    });
    doRemove(create().getId(), new Update() {
      public void update(HasNotNullConstraintsJDO obj) {
        obj.setStr(null);
      }
    });
  }

  private HasNotNullConstraintsJDO create() {
    HasNotNullConstraintsJDO pc = createHasConstraintsJDO(VAL_BOOL, VAL_CHAR,
                                                          VAL_BYTE, VAL_SHORT, VAL_INT, VAL_LONG,
                                                          VAL_FLOAT, VAL_DOUBLE, VAL_STRING);
    makePersistentInTxn(pc, TXN_START_END);
    return pc;
  }

  private void doUpdate(Long id, Update update) {
    try {
      beginTxn();
      HasNotNullConstraintsJDO pc = pm.getObjectById(HasNotNullConstraintsJDO.class, id);
      update.update(pc);
      commitTxn();
      fail("expected Exception");
    } catch (JDOUserException e) {
      // good
      if (pm.currentTransaction().isActive()) {
        rollbackTxn();
      }
    }
  }

  private void doRemove(Long id, Update update) {
    beginTxn();
    HasNotNullConstraintsJDO pc = pm.getObjectById(HasNotNullConstraintsJDO.class, id);
    update.update(pc);
    pm.deletePersistent(pc);
    commitTxn();

    assertEquals(0, countForClass(HasNotNullConstraintsJDO.class));
  }

  private HasNotNullConstraintsJDO createHasConstraintsJDO(Boolean bool, Character c, Byte b,
                                                           Short s, Integer i, Long l,
                                                           Float f, Double d, String str) {
    HasNotNullConstraintsJDO pc = new HasNotNullConstraintsJDO();
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

    void update(HasNotNullConstraintsJDO pc);
  }

}
