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
package com.google.appengine.datanucleus.appengine.query;

import com.google.appengine.api.datastore.Entity;

import com.google.appengine.datanucleus.appengine.Utils.Supplier;

import java.util.Iterator;

/**
 * {@link Iterable} implementation that catches runtime exceptions thrown by
 * the datastore api and translates them to the appropriate JDO or JPA exception.
 * DataNucleus is supposed to do this for us, but they miss runtime exceptions
 * that are thrown while iterating.
 *
 * @author Max Ross <maxr@google.com>
 */
class RuntimeExceptionWrappingIterable implements Iterable<Entity>, RuntimeExceptionObserver {

  private final boolean isJPA;
  private final Supplier<Iterator<Entity>> iteratorSupplier;
  private boolean hasError = false;

  RuntimeExceptionWrappingIterable(final Iterable<Entity> inner, boolean isJPA) {
    if (inner == null) {
      throw new NullPointerException("inner cannot be null");
    }
    this.isJPA = isJPA;
    Supplier<Iterator<Entity>> supplier = QueryExceptionWrappers.datastoreToDataNucleus(
        new Supplier<Iterator<Entity>>() {
          public Iterator<Entity> get() {
            return newIterator(inner.iterator());
          }
        });
    if (isJPA) {
      this.iteratorSupplier = QueryExceptionWrappers.dataNucleusToJPA(supplier);
    } else {
      this.iteratorSupplier = QueryExceptionWrappers.dataNucleusToJDO(supplier);
    }
  }

  public Iterator<Entity> iterator() {
    return iteratorSupplier.get();
  }

  Iterator<Entity> newIterator(Iterator<Entity> innerIter) {
    return new RuntimeExceptionWrappingIterator(innerIter, isJPA, this);
  }

  public void onException() {
    hasError = true;
  }

  boolean hasError() {
    return hasError;
  }
}