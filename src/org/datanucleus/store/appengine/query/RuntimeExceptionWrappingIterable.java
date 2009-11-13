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
package org.datanucleus.store.appengine.query;

import com.google.appengine.api.datastore.DatastoreFailureException;
import com.google.appengine.api.datastore.Entity;

import static org.datanucleus.store.appengine.DatastoreExceptionTranslator.wrapDatastoreFailureException;
import static org.datanucleus.store.appengine.DatastoreExceptionTranslator.wrapIllegalArgumentException;

import java.util.Iterator;

/**
 * {@link Iterable} implementation that catches runtime exceptions thrown by
 * the datastore api and translates them to the appropriate nucleus exception.
 *
 * @author Max Ross <maxr@google.com>
 */
class RuntimeExceptionWrappingIterable implements Iterable<Entity> {

  private final Iterable<Entity> inner;

  RuntimeExceptionWrappingIterable(Iterable<Entity> inner) {
    this.inner = inner;
  }

  public Iterator<Entity> iterator() {
    try {
      return newIterator(inner.iterator());
    } catch (IllegalArgumentException e) {
      throw wrapIllegalArgumentException(e);
    } catch (DatastoreFailureException e) {
      throw wrapDatastoreFailureException(e);
    }
  }

  Iterator<Entity> newIterator(Iterator<Entity> innerIter) {
    return new RuntimeExceptionWrappingIterator(innerIter);
  }
}