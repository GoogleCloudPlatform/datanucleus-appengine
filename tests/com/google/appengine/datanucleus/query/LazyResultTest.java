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
import com.google.appengine.datanucleus.DatastoreTestCase;
import com.google.appengine.datanucleus.Utils;
import com.google.appengine.datanucleus.Utils.Function;


import java.util.Collections;
import java.util.Iterator;
import java.util.ListIterator;

/**
 * @author Max Ross <maxr@google.com>
 */
public class LazyResultTest extends DatastoreTestCase {

  public void testEquality() {
    LazyResult lr1 = new LazyResult<Object>(Collections.<Entity>emptyList(), null, false);
    LazyResult lr2 = new LazyResult<Object>(Collections.<Entity>emptyList(), null, false);
    assertTrue(lr1.equals(lr1));
    assertFalse(lr1.equals(lr2));
  }

  private static final Function<Entity, Object> NULL_FUNC = new Function<Entity, Object>() {
    public Object apply(Entity entity) {
      return entity;
    }
  };

  private static class CountingIterable implements Iterable<Entity> {
    private final Iterable<Entity> iterable;
    private int nextCount = 0;

    private CountingIterable(Iterable<Entity> iterable) {
      this.iterable = iterable;
    }

    public Iterator<Entity> iterator() {
      return new CountingIterator(iterable.iterator());
    }

    private class CountingIterator implements Iterator<Entity> {
      private final Iterator<Entity> iter;

      private CountingIterator(Iterator<Entity> iter) {
        this.iter = iter;
      }

      public boolean hasNext() {
        return iter.hasNext();
      }

      public Entity next() {
        nextCount++;
        return iter.next();
      }

      public void remove() {
        throw new UnsupportedOperationException();
      }
    }
  }

  public void testSize_FreshIterator() {
    CountingIterable iterable = new CountingIterable(Utils.<Entity>newArrayList());
    LazyResult lr = new LazyResult<Object>(iterable, NULL_FUNC, false);
    assertEquals(0, lr.size());
    assertEquals(0, iterable.nextCount);

    Entity e = null;
    iterable = new CountingIterable(Utils.newArrayList(e));
    lr = new LazyResult<Object>(iterable, NULL_FUNC, false);
    assertEquals(1, lr.size());
    assertEquals(1, iterable.nextCount);

    iterable = new CountingIterable(Utils.newArrayList(e, e));
    lr = new LazyResult<Object>(iterable, NULL_FUNC, false);
    assertEquals(2, lr.size());
    assertEquals(2, iterable.nextCount);
  }

  public void testSize_PartiallyConsumedIterator() {
    Entity e = null;
    CountingIterable iterable = new CountingIterable(Utils.newArrayList(e, e, e));
    LazyResult lr = new LazyResult<Object>(iterable, NULL_FUNC, false);
    lr.resolveNext();
    assertEquals(1, iterable.nextCount);
    assertEquals(3, lr.size());
    assertEquals(3, iterable.nextCount);
  }

  public void testSize_ExhaustedIterator() {
    Entity e = null;
    CountingIterable iterable = new CountingIterable(Utils.newArrayList(e, e));
    LazyResult lr = new LazyResult<Object>(iterable, NULL_FUNC, false);
    lr.resolveNext();
    lr.resolveNext();
    assertEquals(2, iterable.nextCount);
    assertEquals(2, lr.size());
    assertEquals(2, iterable.nextCount);
  }

  public void testGet_FreshIterator() {
    CountingIterable iterable = new CountingIterable(Utils.<Entity>newArrayList());
    LazyResult lr = new LazyResult<Object>(iterable, NULL_FUNC, false);
    try {
      lr.get(0);
      fail("expected index out of bounds exception");
    } catch (IndexOutOfBoundsException e) {
      // good
    }
    assertEquals(0, iterable.nextCount);

    Entity e1 = new Entity("yar");
    Entity e2 = new Entity("yar");
    iterable = new CountingIterable(Utils.newArrayList(e1, e2));
    lr = new LazyResult<Object>(iterable, NULL_FUNC, false);
    assertEquals(e1, lr.get(0));
    assertEquals(1, iterable.nextCount);
    assertEquals(e2, lr.get(1));
    assertEquals(2, iterable.nextCount);

    try {
      lr.get(3);
      fail("expected index out of bounds exception");
    } catch (IndexOutOfBoundsException e) {
      // good
    }
    assertEquals(2, iterable.nextCount);
  }

  public void testGet_PartiallyConsumedIterator() {
    Entity e1 = new Entity("yar");
    Entity e2 = new Entity("yar");
    CountingIterable iterable = new CountingIterable(Utils.<Entity>newArrayList(e1, e2));
    LazyResult lr = new LazyResult<Object>(iterable, NULL_FUNC, false);
    lr.resolveNext();
    assertEquals(1, iterable.nextCount);
    assertEquals(e1, lr.get(0));
    assertEquals(1, iterable.nextCount);
    assertEquals(e2, lr.get(1));
    assertEquals(2, iterable.nextCount);

    try {
      lr.get(3);
      fail("expected index out of bounds exception");
    } catch (IndexOutOfBoundsException e) {
      // good
    }
    assertEquals(2, iterable.nextCount);
  }

  public void testGet_ExhaustedIterator() {
    Entity e1 = new Entity("yar");
    Entity e2 = new Entity("yar");
    CountingIterable iterable = new CountingIterable(Utils.<Entity>newArrayList(e1, e2));
    LazyResult lr = new LazyResult<Object>(iterable, NULL_FUNC, false);
    lr.resolveNext();
    lr.resolveNext();
    assertEquals(2, iterable.nextCount);
    assertEquals(e1, lr.get(0));
    assertEquals(2, iterable.nextCount);
    assertEquals(e2, lr.get(1));
    assertEquals(2, iterable.nextCount);

    try {
      lr.get(3);
      fail("expected index out of bounds exception");
    } catch (IndexOutOfBoundsException e) {
      // good
    }
    assertEquals(2, iterable.nextCount);
  }

  // This implicitly tests the iterator() method as well since iterator() just
  // delegates to listIterator()
  public void testListIterator() {
    CountingIterable iterable = new CountingIterable(Utils.<Entity>newArrayList());
    LazyResult lr = new LazyResult<Object>(iterable, NULL_FUNC, false);
    assertFalse(lr.listIterator().hasNext());

    Entity e1 = new Entity("yar1");
    Entity e2 = new Entity("yar2");
    Entity e3 = new Entity("yar3");
    Entity e4 = new Entity("yar4");
    iterable = new CountingIterable(Utils.<Entity>newArrayList(e1, e2, e3, e4));
    lr = new LazyResult<Object>(iterable, NULL_FUNC, false);

    ListIterator listIter = lr.listIterator();
    assertTrue(listIter.hasNext());
    assertSame(e1, listIter.next());
    assertEquals(1, iterable.nextCount);
    assertSame(e1, lr.get(0));
    assertEquals(1, iterable.nextCount);

    assertEquals(e2, listIter.next());
    assertEquals(2, iterable.nextCount);
    // Calls to the iterator make more data available to get() so nextCount
    // does not increment.
    assertSame(e2, lr.get(1));
    assertEquals(2, iterable.nextCount);

    // now we work our way backwards
    assertEquals(2, listIter.nextIndex());
    assertEquals(1, listIter.previousIndex());
    assertTrue(listIter.hasPrevious());
    assertTrue(listIter.hasNext());
    assertSame(e2, listIter.previous());
    assertTrue(listIter.hasNext());
    assertEquals(3, iterable.nextCount);
    assertTrue(listIter.hasPrevious());

    assertEquals(1, listIter.nextIndex());
    assertEquals(0, listIter.previousIndex());
    assertSame(e1, listIter.previous());
    assertEquals(3, iterable.nextCount);
    assertFalse(listIter.hasPrevious());
    assertTrue(listIter.hasNext());
    assertEquals(-1, listIter.previousIndex());

    // now we go forwards again
    assertSame(e1, listIter.next());
    assertEquals(1, listIter.nextIndex());
    assertEquals(0, listIter.previousIndex());
    assertEquals(3, iterable.nextCount);
    assertTrue(listIter.hasPrevious());
    assertTrue(listIter.hasNext());

    assertSame(e2, listIter.next());
    assertEquals(2, listIter.nextIndex());
    assertEquals(1, listIter.previousIndex());
    assertEquals(3, iterable.nextCount);
    assertTrue(listIter.hasPrevious());
    assertTrue(listIter.hasNext());
    assertEquals(3, iterable.nextCount);

    assertSame(e3, listIter.next());
    assertEquals(3, iterable.nextCount);
    assertEquals(3, listIter.nextIndex());
    assertEquals(2, listIter.previousIndex());
    assertTrue(listIter.hasPrevious());
    assertTrue(listIter.hasNext());
    // the call to hasNext() results in a fetch
    assertEquals(4, iterable.nextCount);

    assertSame(e4, listIter.next());
    assertEquals(4, listIter.nextIndex());
    assertEquals(3, listIter.previousIndex());
    assertEquals(4, iterable.nextCount);
    assertTrue(listIter.hasPrevious());
    assertFalse(listIter.hasNext());

    iterable = new CountingIterable(Utils.<Entity>newArrayList(e1, e2));
    lr = new LazyResult<Object>(iterable, NULL_FUNC, false);
    listIter = lr.listIterator();
    assertTrue(listIter.hasNext());
    assertEquals(e1, listIter.next());
    assertEquals(1, iterable.nextCount);

    // Call to get makes more data available to the iterator.
    lr.get(1);
    assertEquals(2, iterable.nextCount);
    assertTrue(listIter.hasNext());
    assertSame(e2, listIter.next());
    assertEquals(2, iterable.nextCount);
  }

}
