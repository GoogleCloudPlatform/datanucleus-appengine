/*
 * Copyright (C) 2010 Google Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.appengine.datanucleus.query;

import com.google.appengine.api.datastore.DatastoreFailureException;
import com.google.appengine.api.datastore.DatastoreTimeoutException;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.datanucleus.DatastoreTestCase;

import org.datanucleus.api.ApiAdapter;
import org.datanucleus.api.jdo.JDOAdapter;
import org.datanucleus.api.jpa.JPAAdapter;
import org.datanucleus.store.query.QueryTimeoutException;
import org.easymock.EasyMock;

import java.util.Arrays;
import java.util.Iterator;

import javax.jdo.JDODataStoreException;
import javax.jdo.JDOFatalUserException;
import javax.persistence.PersistenceException;

/**
 * @author Max Ross <max.ross@gmail.com>
 */
public class RuntimeExceptionWrappingIteratorTest extends DatastoreTestCase {

  private boolean receivedException = false;
  private RuntimeExceptionObserver observer = new RuntimeExceptionObserver() {
    public void onException() {
      receivedException = true;
    }
  };
  private Iterator<Entity> iter;

  protected void setUpIterator(RuntimeException rte) {
    receivedException = false;
    iter = EasyMock.createMock(Iterator.class);
    EasyMock.expect(iter.hasNext()).andThrow(rte);
    EasyMock.expect(iter.next()).andThrow(rte);
    iter.remove();
    EasyMock.expectLastCall().andThrow(rte);
    EasyMock.replay(iter);
  }

  public void testNoExceptionsJPA() {
    Entity e1 = new Entity("foo");
    Entity e2 = new Entity("foo");
    Entity e3 = new Entity("foo");
    ApiAdapter api = new JPAAdapter();
    RuntimeExceptionWrappingIterator rewi =
        new RuntimeExceptionWrappingIterator(api, Arrays.asList(e1, e2, e3).iterator(), observer);
    int count = 0;
    while (rewi.hasNext()) {
      rewi.next();
      count++;
    }
    assertEquals(3, count);
  }

  public void testExceptionsJPA_IllegalArg() {
    setUpIterator(new IllegalArgumentException("boom"));
    ApiAdapter api = new JPAAdapter();
    RuntimeExceptionWrappingIterator rewi =
        new RuntimeExceptionWrappingIterator(api, iter, observer);
    try {
      rewi.hasNext();
      fail("expected exception");
    } catch (PersistenceException pe) {
      // good
      assertTrue(pe.getCause() instanceof IllegalArgumentException);
      assertEquals(pe.getCause().getMessage(), "boom");
      assertTrue(receivedException);
      receivedException = false;
    }
    try {
      rewi.next();
      fail("expected exception");
    } catch (PersistenceException pe) {
      // good
      assertTrue(pe.getCause() instanceof IllegalArgumentException);
      assertEquals(pe.getCause().getMessage(), "boom");
      assertTrue(receivedException);
      receivedException = false;
    }
    try {
      rewi.remove();
      fail("expected exception");
    } catch (PersistenceException pe) {
      // good
      assertTrue(pe.getCause() instanceof IllegalArgumentException);
      assertEquals(pe.getCause().getMessage(), "boom");
      assertTrue(receivedException);
      receivedException = false;
    }
  }

  public void testExceptionsJPA_DatastoreFailure() {
    setUpIterator(new DatastoreFailureException("boom"));
    ApiAdapter api = new JPAAdapter();
    RuntimeExceptionWrappingIterator rewi =
        new RuntimeExceptionWrappingIterator(api, iter, observer);
    try {
      rewi.hasNext();
      fail("expected exception");
    } catch (PersistenceException pe) {
      // good
      assertTrue(pe.getCause() instanceof DatastoreFailureException);
      assertEquals(pe.getCause().getMessage(), "boom");
      assertTrue(receivedException);
      receivedException = false;
    }
    try {
      rewi.next();
      fail("expected exception");
    } catch (PersistenceException pe) {
      // good
      assertTrue(pe.getCause() instanceof DatastoreFailureException);
      assertEquals(pe.getCause().getMessage(), "boom");
      assertTrue(receivedException);
      receivedException = false;
    }
    try {
      rewi.remove();
      fail("expected exception");
    } catch (PersistenceException pe) {
      // good
      assertTrue(pe.getCause() instanceof DatastoreFailureException);
      assertEquals(pe.getCause().getMessage(), "boom");
      assertTrue(receivedException);
      receivedException = false;
    }
  }

  public void testExceptionsJPA_Timeout() {
    setUpIterator(new DatastoreTimeoutException("boom"));
    ApiAdapter api = new JPAAdapter();
    RuntimeExceptionWrappingIterator rewi =
        new RuntimeExceptionWrappingIterator(api, iter, observer);
    try {
      rewi.hasNext();
      fail("expected exception");
    } catch (javax.persistence.QueryTimeoutException qte) {
      // good
      assertTrue(qte.getCause() instanceof QueryTimeoutException);
      assertTrue(qte.getCause().getCause() instanceof DatastoreTimeoutException);
      assertEquals(qte.getCause().getCause().getMessage(), "boom");
      assertTrue(receivedException);
      receivedException = false;
    }
    try {
      rewi.next();
      fail("expected exception");
    } catch (javax.persistence.QueryTimeoutException qte) {
      // good
      assertTrue(qte.getCause() instanceof QueryTimeoutException);
      assertTrue(qte.getCause().getCause() instanceof DatastoreTimeoutException);
      assertEquals(qte.getCause().getCause().getMessage(), "boom");
      assertTrue(receivedException);
      receivedException = false;
    }
    try {
      rewi.remove();
      fail("expected exception");
    } catch (javax.persistence.QueryTimeoutException qte) {
      // good
      assertTrue(qte.getCause() instanceof QueryTimeoutException);
      assertTrue(qte.getCause().getCause() instanceof DatastoreTimeoutException);
      assertEquals(qte.getCause().getCause().getMessage(), "boom");
      assertTrue(receivedException);
      receivedException = false;
    }
  }
  public void testNoExceptionsJDO() {
    Entity e1 = new Entity("foo");
    Entity e2 = new Entity("foo");
    Entity e3 = new Entity("foo");
    ApiAdapter api = new JDOAdapter();
    RuntimeExceptionWrappingIterator rewi =
        new RuntimeExceptionWrappingIterator(api, Arrays.asList(e1, e2, e3).iterator(), observer);
    int count = 0;
    while (rewi.hasNext()) {
      rewi.next();
      count++;
    }
    assertEquals(3, count);
  }

  public void testExceptionsJDO_IllegalArg() {
    setUpIterator(new IllegalArgumentException("boom"));
    ApiAdapter api = new JDOAdapter();
    RuntimeExceptionWrappingIterator rewi =
        new RuntimeExceptionWrappingIterator(api, iter, observer);
    try {
      rewi.hasNext();
      fail("expected exception");
    } catch (JDOFatalUserException jfue) {
      // good
      assertTrue(jfue.getCause() instanceof IllegalArgumentException);
      assertEquals(jfue.getCause().getMessage(), "boom");
      assertTrue(receivedException);
      receivedException = false;
    }
    try {
      rewi.next();
      fail("expected exception");
    } catch (JDOFatalUserException jfue) {
      // good
      assertTrue(jfue.getCause() instanceof IllegalArgumentException);
      assertEquals(jfue.getCause().getMessage(), "boom");
      assertTrue(receivedException);
      receivedException = false;
    }
    try {
      rewi.remove();
      fail("expected exception");
    } catch (JDOFatalUserException jfue) {
      // good
      assertTrue(jfue.getCause() instanceof IllegalArgumentException);
      assertEquals(jfue.getCause().getMessage(), "boom");
      assertTrue(receivedException);
      receivedException = false;
    }
  }

  public void testExceptionsJDO_DatastoreFailure() {
    setUpIterator(new DatastoreFailureException("boom"));
    ApiAdapter api = new JDOAdapter();
    RuntimeExceptionWrappingIterator rewi =
        new RuntimeExceptionWrappingIterator(api, iter, observer);
    try {
      rewi.hasNext();
      fail("expected exception");
    } catch (JDODataStoreException jdse) {
      // good
      assertTrue(jdse.getCause() instanceof DatastoreFailureException);
      assertEquals(jdse.getCause().getMessage(), "boom");
      assertTrue(receivedException);
      receivedException = false;
    }
    try {
      rewi.next();
      fail("expected exception");
    } catch (JDODataStoreException jdse) {
      // good
      assertTrue(jdse.getCause() instanceof DatastoreFailureException);
      assertEquals(jdse.getCause().getMessage(), "boom");
      assertTrue(receivedException);
      receivedException = false;
    }
    try {
      rewi.remove();
      fail("expected exception");
    } catch (JDODataStoreException jdse) {
      // good
      assertTrue(jdse.getCause() instanceof DatastoreFailureException);
      assertEquals(jdse.getCause().getMessage(), "boom");
      assertTrue(receivedException);
      receivedException = false;
    }
  }

  public void testExceptionsJDO_Timeout() {
    setUpIterator(new DatastoreTimeoutException("boom"));
    ApiAdapter api = new JDOAdapter();
    RuntimeExceptionWrappingIterator rewi =
        new RuntimeExceptionWrappingIterator(api, iter, observer);
    try {
      rewi.hasNext();
      fail("expected exception");
    } catch (JDODataStoreException jqte) {
      // good
      assertTrue(jqte.getCause() instanceof QueryTimeoutException);
      assertTrue(jqte.getCause().getCause() instanceof DatastoreTimeoutException);
      assertEquals(jqte.getCause().getCause().getMessage(), "boom");
      assertTrue(receivedException);
      receivedException = false;
    }
    try {
      rewi.next();
      fail("expected exception");
    } catch (JDODataStoreException jqte) {
      // good
      assertTrue(jqte.getCause() instanceof QueryTimeoutException);
      assertTrue(jqte.getCause().getCause() instanceof DatastoreTimeoutException);
      assertEquals(jqte.getCause().getCause().getMessage(), "boom");
      assertTrue(receivedException);
      receivedException = false;
    }
    try {
      rewi.remove();
      fail("expected exception");
    } catch (JDODataStoreException jqte) {
      // good
      assertTrue(jqte.getCause() instanceof QueryTimeoutException);
      assertTrue(jqte.getCause().getCause() instanceof DatastoreTimeoutException);
      assertEquals(jqte.getCause().getCause().getMessage(), "boom");
      assertTrue(receivedException);
      receivedException = false;
    }
  }
}
