// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine.query;

import com.google.apphosting.api.datastore.Query;

/**
 * Simple struct that contains information about a filter that was added to a
 * {@link Query}.  This data is only exposed to tests, and is necessary because
 * the {@link Query} class is final and therefore not mockable.
 *
 * @author Max Ross <maxr@google.com>
 */
class AddedFilter {
   final String propName;
   final Query.FilterOperator op;
   final Object value;

  AddedFilter(String propName, Query.FilterOperator op, Object value) {
    this.propName = propName;
    this.value = value;
    this.op = op;
  }

  @Override
  public String toString() {
    return String.format("%s, %s, %s", propName, op, value);
  }

  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    AddedFilter that = (AddedFilter) o;

    if (op != that.op) {
      return false;
    }
    if (!propName.equals(that.propName)) {
      return false;
    }
    if (!value.equals(that.value)) {
      return false;
    }

    return true;
  }

  public int hashCode() {
    int result;
    result = propName.hashCode();
    result = 31 * result + op.hashCode();
    result = 31 * result + value.hashCode();
    return result;
  }
}
