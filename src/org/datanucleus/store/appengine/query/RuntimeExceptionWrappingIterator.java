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
import com.google.appengine.api.datastore.DatastoreTimeoutException;
import com.google.appengine.api.datastore.Entity;

import static org.datanucleus.store.appengine.DatastoreExceptionTranslator.wrapDatastoreFailureException;
import static org.datanucleus.store.appengine.DatastoreExceptionTranslator.wrapDatastoreTimeoutExceptionForQuery;
import static org.datanucleus.store.appengine.DatastoreExceptionTranslator.wrapIllegalArgumentException;

import java.util.Iterator;

/**
 * {@link Iterator} implementation that catches runtime exceptions thrown by
 * the datastore api and translates them to the appropriate nucleus exception.
 *
 * @author Max Ross <maxr@google.com>
 */
class RuntimeExceptionWrappingIterator implements Iterator<Entity> {

  final Iterator<Entity> inner;

  RuntimeExceptionWrappingIterator(Iterator<Entity> inner) {
    this.inner = inner;
  }

  public boolean hasNext() {
    try {
      return inner.hasNext();
    } catch (IllegalArgumentException e) {
      throw wrapIllegalArgumentException(e);
    } catch (DatastoreTimeoutException e) {
      throw wrapDatastoreTimeoutExceptionForQuery(e);
    } catch (DatastoreFailureException e) {
      throw wrapDatastoreFailureException(e);
    }
  }

  public Entity next() {
    try {
      return inner.next();
    } catch (IllegalArgumentException e) {
      throw wrapIllegalArgumentException(e);
    } catch (DatastoreFailureException e) {
      throw wrapDatastoreFailureException(e);
    }
  }

  public void remove() {
    try {
      inner.remove();
    } catch (IllegalArgumentException e) {
      throw wrapIllegalArgumentException(e);
    } catch (DatastoreFailureException e) {
      throw wrapDatastoreFailureException(e);
    }
  }
}