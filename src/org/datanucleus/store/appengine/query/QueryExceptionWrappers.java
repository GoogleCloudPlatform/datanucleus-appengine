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
package org.datanucleus.store.appengine.query;

import com.google.appengine.api.datastore.DatastoreFailureException;
import com.google.appengine.api.datastore.DatastoreTimeoutException;

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.jdo.NucleusJDOHelper;
import org.datanucleus.jpa.NucleusJPAHelper;
import org.datanucleus.store.appengine.Utils;
import org.datanucleus.store.appengine.Utils.Supplier;
import org.datanucleus.store.query.QueryTimeoutException;

import javax.jdo.JDOException;

import static org.datanucleus.store.appengine.DatastoreExceptionTranslator.wrapDatastoreFailureException;
import static org.datanucleus.store.appengine.DatastoreExceptionTranslator.wrapDatastoreTimeoutExceptionForQuery;
import static org.datanucleus.store.appengine.DatastoreExceptionTranslator.wrapIllegalArgumentException;

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


  static final class JDOQueryTimeoutException extends javax.jdo.JDOQueryTimeoutException {
    private final Throwable cause;

    JDOQueryTimeoutException(Throwable cause) {
      this.cause = cause;
    }

    @Override
    public Throwable getCause() {
      // hack to make the cause available (DataNucleus' JDOQueryTimeoutException
      // sets it to null).
      return cause;
    }
  }

  /**
   * Translates DataNucleus runtime exceptions to JDO runtime exceptions.
   */
  static <T> Supplier<T> dataNucleusToJDO(final Supplier<T> supplier) {
    return new Supplier<T>() {
      public T get() {
        try {
          return supplier.get();
        } catch (QueryTimeoutException qte) {
          throw new JDOQueryTimeoutException(qte);
        } catch (NucleusException ne) {
          throw NucleusJDOHelper.getJDOExceptionForNucleusException(ne);
        }
      }
    };
  }

  /**
   * Translates DataNucleus runtime exceptions to JPA runtime exceptions. 
   */
  static <T> Supplier<T> dataNucleusToJPA(final Supplier<T> supplier) {
    return new Supplier<T>() {
      public T get() {
        try {
          return supplier.get();
        } catch (QueryTimeoutException qte) {
          RuntimeException rte = new javax.persistence.QueryTimeoutException();
          rte.initCause(qte);
          throw rte;
        } catch (NucleusException ne) {
          throw NucleusJPAHelper.getJPAExceptionForNucleusException(ne);
        } catch (JDOException je) {
          throw NucleusJPAHelper.getJPAExceptionForJDOException(je);
        }
      }
    };
  }
}
