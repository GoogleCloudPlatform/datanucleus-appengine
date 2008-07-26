// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine.query;

import com.google.apphosting.api.datastore.Query;

/**
 * Simple struct that contains information about a sort that was added to a
 * {@link Query}.  This data is only exposed to tests, and is necessary because
 * the {@link Query} class is final and therefore not mockable.
 *
 * @author Max Ross <maxr@google.com>
 */
class AddedSort {
  private final String propName;
  private final Query.SortDirection sortDir;

  AddedSort(String propName, Query.SortDirection sortDir) {
    this.propName = propName;
    this.sortDir = sortDir;
  }

  @Override
  public String toString() {
    return String.format("%s, %s", propName, sortDir);
  }

  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    AddedSort addedSort = (AddedSort) o;

    if (!propName.equals(addedSort.propName)) {
      return false;
    }
    if (sortDir != addedSort.sortDir) {
      return false;
    }

    return true;
  }

  public int hashCode() {
    int result;
    result = propName.hashCode();
    result = 31 * result + sortDir.hashCode();
    return result;
  }
}