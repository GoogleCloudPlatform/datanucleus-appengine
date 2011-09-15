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
package com.google.appengine.datanucleus;

import com.google.appengine.api.datastore.DatastoreAttributes;
import com.google.appengine.api.datastore.DatastoreFailureException;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Index;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyRange;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Transaction;
import com.google.appengine.api.datastore.TransactionOptions;

import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;

import static com.google.appengine.datanucleus.DatastoreExceptionTranslator.wrapConcurrentModificationException;
import static com.google.appengine.datanucleus.DatastoreExceptionTranslator.wrapDatastoreFailureException;
import static com.google.appengine.datanucleus.DatastoreExceptionTranslator.wrapIllegalArgumentException;

/**
 * {@link DatastoreService} implementation that catches runtime exceptions
 * thrown and wraps them in the appropriate DataNucleus exception.
 *
 * @author Max Ross <maxr@google.com>
 */
public class WrappedDatastoreService implements DatastoreService {

  private final DatastoreService inner;

  public WrappedDatastoreService(DatastoreService inner) {
    this.inner = inner;
  }

  public DatastoreService getDelegate() {
    return inner;
  }

  public Entity get(Key key) throws EntityNotFoundException {
    try {
      return inner.get(key);
    } catch (IllegalArgumentException e) {
      throw wrapIllegalArgumentException(e);
    } catch (DatastoreFailureException e) {
      throw wrapDatastoreFailureException(e);
    }
  }

  public Entity get(Transaction transaction, Key key) throws EntityNotFoundException {
    try {
      return inner.get(transaction, key);
    } catch (IllegalArgumentException e) {
      throw wrapIllegalArgumentException(e);
    } catch (DatastoreFailureException e) {
      throw wrapDatastoreFailureException(e);
    }
  }

  public Map<Key, Entity> get(Iterable<Key> keyIterable) {
    try {
      return inner.get(keyIterable);
    } catch (IllegalArgumentException e) {
      throw wrapIllegalArgumentException(e);
    } catch (DatastoreFailureException e) {
      throw wrapDatastoreFailureException(e);
    }
  }

  public Map<Key, Entity> get(Transaction transaction, Iterable<Key> keyIterable) {
    try {
      return inner.get(transaction, keyIterable);
    } catch (IllegalArgumentException e) {
      throw wrapIllegalArgumentException(e);
    } catch (DatastoreFailureException e) {
      throw wrapDatastoreFailureException(e);
    }
  }

  public Key put(Entity entity) {
    try {
      return inner.put(entity);
    } catch (IllegalArgumentException e) {
      throw wrapIllegalArgumentException(e);
    } catch (ConcurrentModificationException e) {
      throw wrapConcurrentModificationException(e);
    } catch (DatastoreFailureException e) {
      throw wrapDatastoreFailureException(e);
    }
  }

  public Key put(Transaction transaction, Entity entity) {
    try {
      return inner.put(transaction, entity);
    } catch (IllegalArgumentException e) {
      throw wrapIllegalArgumentException(e);
    } catch (ConcurrentModificationException e) {
      throw wrapConcurrentModificationException(e);
    } catch (DatastoreFailureException e) {
      throw wrapDatastoreFailureException(e);
    }
  }

  public List<Key> put(Iterable<Entity> entityIterable) {
    try {
      return inner.put(entityIterable);
    } catch (IllegalArgumentException e) {
      throw wrapIllegalArgumentException(e);
    } catch (ConcurrentModificationException e) {
      throw wrapConcurrentModificationException(e);
    } catch (DatastoreFailureException e) {
      throw wrapDatastoreFailureException(e);
    }
  }

  public List<Key> put(Transaction transaction, Iterable<Entity> entityIterable) {
    try {
      return inner.put(transaction, entityIterable);
    } catch (IllegalArgumentException e) {
      throw wrapIllegalArgumentException(e);
    } catch (ConcurrentModificationException e) {
      throw wrapConcurrentModificationException(e);
    } catch (DatastoreFailureException e) {
      throw wrapDatastoreFailureException(e);
    }
  }

  public void delete(Key... keys) {
    try {
      inner.delete(keys);
    } catch (IllegalArgumentException e) {
      throw wrapIllegalArgumentException(e);
    } catch (ConcurrentModificationException e) {
      throw wrapConcurrentModificationException(e);
    } catch (DatastoreFailureException e) {
      throw wrapDatastoreFailureException(e);
    }
  }

  public void delete(Transaction transaction, Key... keys) {
    try {
      inner.delete(transaction, keys);
    } catch (IllegalArgumentException e) {
      throw wrapIllegalArgumentException(e);
    } catch (ConcurrentModificationException e) {
      throw wrapConcurrentModificationException(e);
    } catch (DatastoreFailureException e) {
      throw wrapDatastoreFailureException(e);
    }
  }

  public void delete(Iterable<Key> keyIterable) {
    try {
      inner.delete(keyIterable);
    } catch (IllegalArgumentException e) {
      throw wrapIllegalArgumentException(e);
    } catch (ConcurrentModificationException e) {
      throw wrapConcurrentModificationException(e);
    } catch (DatastoreFailureException e) {
      throw wrapDatastoreFailureException(e);
    }
  }

  public void delete(Transaction transaction, Iterable<Key> keyIterable) {
    try {
      inner.delete(transaction, keyIterable);
    } catch (IllegalArgumentException e) {
      throw wrapIllegalArgumentException(e);
    } catch (ConcurrentModificationException e) {
      throw wrapConcurrentModificationException(e);
    } catch (DatastoreFailureException e) {
      throw wrapDatastoreFailureException(e);
    }
  }

  public PreparedQuery prepare(Query query) {
    try {
      return inner.prepare(query);
    } catch (IllegalArgumentException e) {
      throw wrapIllegalArgumentException(e);
    } catch (DatastoreFailureException e) {
      throw wrapDatastoreFailureException(e);
    }
  }

  public PreparedQuery prepare(Transaction transaction, Query query) {
    try {
      return inner.prepare(transaction, query);
    } catch (IllegalArgumentException e) {
      throw wrapIllegalArgumentException(e);
    } catch (DatastoreFailureException e) {
      throw wrapDatastoreFailureException(e);
    }
  }

  public Transaction beginTransaction() {
    try {
      return inner.beginTransaction();
    } catch (IllegalArgumentException e) {
      throw wrapIllegalArgumentException(e);
    } catch (DatastoreFailureException e) {
      throw wrapDatastoreFailureException(e);
    }
  }

  public Transaction beginTransaction(TransactionOptions transactionOptions) {
    try {
      return inner.beginTransaction(transactionOptions);
    } catch (IllegalArgumentException e) {
      throw wrapIllegalArgumentException(e);
    } catch (DatastoreFailureException e) {
      throw wrapDatastoreFailureException(e);
    }
  }

  public Transaction getCurrentTransaction() {
    try {
      return inner.getCurrentTransaction();
    } catch (IllegalArgumentException e) {
      throw wrapIllegalArgumentException(e);
    } catch (DatastoreFailureException e) {
      throw wrapDatastoreFailureException(e);
    }
  }

  public Transaction getCurrentTransaction(Transaction transaction) {
    try {
      return inner.getCurrentTransaction(transaction);
    } catch (IllegalArgumentException e) {
      throw wrapIllegalArgumentException(e);
    } catch (DatastoreFailureException e) {
      throw wrapDatastoreFailureException(e);
    }
  }

  public Collection<Transaction> getActiveTransactions() {
    try {
      return inner.getActiveTransactions();
    } catch (IllegalArgumentException e) {
      throw wrapIllegalArgumentException(e);
    } catch (DatastoreFailureException e) {
      throw wrapDatastoreFailureException(e);
    }      
  }

  public KeyRange allocateIds(String kind, long num) {
    try {
      return inner.allocateIds(kind, num);
    } catch (IllegalArgumentException e) {
      throw wrapIllegalArgumentException(e);
    } catch (DatastoreFailureException e) {
      throw wrapDatastoreFailureException(e);
    }
  }

  public KeyRange allocateIds(Key parent, String kind, long num) {
    try {
      return inner.allocateIds(parent, kind, num);
    } catch (IllegalArgumentException e) {
      throw wrapIllegalArgumentException(e);
    } catch (DatastoreFailureException e) {
      throw wrapDatastoreFailureException(e);
    }
  }

  public KeyRangeState allocateIdRange(KeyRange keyRange) {
    try {
      return inner.allocateIdRange(keyRange);
    } catch (IllegalArgumentException e) {
      throw wrapIllegalArgumentException(e);
    } catch (DatastoreFailureException e) {
      throw wrapDatastoreFailureException(e);
    }
  }

  public DatastoreAttributes getDatastoreAttributes() {
    try {
      return inner.getDatastoreAttributes();
    } catch (IllegalArgumentException e) {
      throw wrapIllegalArgumentException(e);
    } catch (DatastoreFailureException e) {
      throw wrapDatastoreFailureException(e);
    }
  }

  public Map<Index, Index.IndexState> getIndexes() {
    try {
      return inner.getIndexes();
    } catch (IllegalArgumentException e) {
      throw wrapIllegalArgumentException(e);
    } catch (DatastoreFailureException e) {
      throw wrapDatastoreFailureException(e);
    }
  }
}
