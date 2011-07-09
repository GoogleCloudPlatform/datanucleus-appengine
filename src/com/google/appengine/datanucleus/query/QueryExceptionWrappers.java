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

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.api.ApiAdapter;
import org.datanucleus.store.query.QueryTimeoutException;

import com.google.appengine.datanucleus.Utils;
import com.google.appengine.datanucleus.Utils.Supplier;

import static com.google.appengine.datanucleus.DatastoreExceptionTranslator.wrapDatastoreFailureException;
import static com.google.appengine.datanucleus.DatastoreExceptionTranslator.wrapDatastoreTimeoutExceptionForQuery;
import static com.google.appengine.datanucleus.DatastoreExceptionTranslator.wrapIllegalArgumentException;

/**
 * Helper methods that wrap the execution of the provided {@link Utils.Supplier}
 * with exception handling logic that translates from one set of exceptions to
 * another.
 *
 * @author Max Ross <max.ross@gmail.com>
 */
final class QueryExceptionWrappers {
  private QueryExceptionWrappers() { }

  /**
   * Translates datastore runtime exceptions to DataNucleus runtime exceptions.
   */
  static <T> Supplier<T> datastoreToDataNucleus(final Supplier<T> supplier) {
    return new Supplier<T>() {
      public T get() {
        try {
          return supplier.get();
        } catch (IllegalArgumentException e) {
          throw wrapIllegalArgumentException(e);
        } catch (DatastoreTimeoutException e) {
          throw wrapDatastoreTimeoutExceptionForQuery(e);
        } catch (DatastoreFailureException e) {
          throw wrapDatastoreFailureException(e);
        }
      }
    };
  }

  /**
   * Translates DataNucleus runtime exceptions to Api runtime exceptions.
   */
  static <T> Supplier<T> dataNucleusToApi(final ApiAdapter api, final Supplier<T> supplier) {
    return new Supplier<T>() {
      public T get() {
        try {
          return supplier.get();
        } catch (QueryTimeoutException te) {
          throw api.getApiExceptionForNucleusException(te);
        } catch (NucleusException ne) {
          throw api.getApiExceptionForNucleusException(ne);
        }
      }
    };
  }
}
