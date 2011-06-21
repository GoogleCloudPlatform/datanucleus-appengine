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

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.Query;

import com.google.appengine.datanucleus.Utils;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * A userland implementation of joins that only supports equality filters.
 * This could be done more efficiently if we exposed index queries in the
 * datastore api (queries that return only index data).
 *
 * TODO(maxr): Optimize the child query to take the first child key of the
 * first parent entity into account.
 *
 * TODO(maxr): Support n-way joins.
 *
 * @author Max Ross <maxr@google.com>
 */
final class JoinHelper {

  /**
   * Child keys that we've already consumed.  We maintain this here instead of
   * the {@link Iterable} so that we can look at it in our tests.
   */
  private final Set<Key> materializedChildKeys = Utils.newHashSet();

  /**
   * Execute the join query described by the provided {@link QueryData}.
   *
   * @param qd The {@link QueryData} describing the join query to execute.
   * @param query The datanucleus representation of the query to execute.
   * @param ds The datastore service to use.
   * @param opts The fetch options to apply to the query.  Can be {@code null}.
   * @return The query result.
   */
  Iterable<Entity> executeJoinQuery(QueryData qd, DatastoreQuery query, DatastoreService ds, FetchOptions opts) {
    // make sure we're starting fresh
    materializedChildKeys.clear();
    // We need to sort by the join column, but make sure the user hasn't added
    // this sort explicitly.
    String joinSortProp = query.getSortProperty(qd, qd.joinOrderExpression);

    validateJoinQuery(qd, query, joinSortProp);

    if (!sortAlreadyExists(joinSortProp, Query.SortDirection.ASCENDING, qd.primaryDatastoreQuery)) {
      qd.primaryDatastoreQuery.addSort(joinSortProp);
    }

    Integer chunkSize = null;
    Integer prefetchSize = null;
    Integer offset = null;
    Integer limit = null;
    if (opts != null) {
      chunkSize = opts.getChunkSize();
      prefetchSize = opts.getPrefetchSize();
      offset = opts.getOffset();
      limit = opts.getLimit();
    }
    // We'll need to apply the offset and the limit in-memory but we can at least
    // make use of the chunk and prefetch size.
    FetchOptions optsWithoutOffsetAndLimit =
        getFetchOptionsWithoutOffsetAndLimit(opts, chunkSize, prefetchSize);
    String keyProperty = qd.primaryDatastoreQuery.getSortPredicates().get(0).getPropertyName();
    Iterable<Entity> primaryResult;
    Iterator<Entity> joinResult;
    if (optsWithoutOffsetAndLimit == null) {
      primaryResult = ds.prepare(qd.primaryDatastoreQuery).asIterable();
      joinResult = ds.prepare(qd.joinQuery).asIterator();
    } else {
      primaryResult = ds.prepare(qd.primaryDatastoreQuery).asIterable(optsWithoutOffsetAndLimit);
      joinResult = ds.prepare(qd.joinQuery).asIterator(optsWithoutOffsetAndLimit);
    }
    Iterable<Entity> result = mergeJoin(keyProperty, primaryResult, joinResult);
    if (offset == null && limit == null) {
      return result;
    }
    return new SlicingIterable<Entity>(offset == null ? 0 : offset, limit,
                                       mergeJoin(keyProperty, primaryResult, joinResult));
  }

  private FetchOptions getFetchOptionsWithoutOffsetAndLimit(
      FetchOptions opts, Integer chunkSize, Integer prefetchSize) {
    FetchOptions optsWithoutOffsetAndLimit = null;
    if (chunkSize != null) {
      optsWithoutOffsetAndLimit = FetchOptions.Builder.withChunkSize(opts.getChunkSize());
      if (prefetchSize != null) {
        optsWithoutOffsetAndLimit.prefetchSize(opts.getPrefetchSize());
      }
    } else if (prefetchSize != null) {
      optsWithoutOffsetAndLimit = FetchOptions.Builder.withPrefetchSize(prefetchSize);
    }
    return optsWithoutOffsetAndLimit;
  }

  private void validateJoinQuery(QueryData qd, DatastoreQuery query, String joinSortProp) {
    // all filters on the primary must be equality
    for (Query.FilterPredicate fp : qd.primaryDatastoreQuery.getFilterPredicates()) {
      if (fp.getOperator() != Query.FilterOperator.EQUAL) {
        throw query.new UnsupportedDatastoreFeatureException(
            "Filter on property '" + fp.getPropertyName() + "' uses operator '" + fp.getOperator()
            + "'.  Joins are only supported when all filters are 'equals' filters.");
      }
    }

    // all filters on the join must be equality
    for (Query.FilterPredicate fp : qd.joinQuery.getFilterPredicates()) {
      if (fp.getOperator() != Query.FilterOperator.EQUAL) {
        throw query.new UnsupportedDatastoreFeatureException(
            "Filter on property '" + fp.getPropertyName() + "' uses operator '" + fp.getOperator()
            + "'.  Joins are only supported when all filters are 'equals' filters.");
      }
    }

    List<Query.SortPredicate> primarySorts = qd.primaryDatastoreQuery.getSortPredicates();

    // There must be 0 or 1 sort orders total.
    // If there is a sort order it must be on the join column in ascending order.
    // TODO(maxr): support sorting by join column in descending order
    if (primarySorts.size() > 1 || !qd.joinQuery.getSortPredicates().isEmpty() ||
        (!primarySorts.isEmpty() &&
        (!primarySorts.get(0).getPropertyName().equals(joinSortProp) ||
         primarySorts.get(0).getDirection() != Query.SortDirection.ASCENDING))) {
      throw query.new UnsupportedDatastoreFeatureException(
          "Joins can only be sorted by the join column in "
          + "ascending order (in this case '" + joinSortProp +"')");
    }    
  }

  Iterable<Entity> mergeJoin(String joinProperty, Iterable<Entity> parents, Iterator<Entity> childIter) {
    return new StreamingMergeJoinResult(new MergeJoinIterable(joinProperty, parents, childIter));
  }

  Set<Key> getMaterializedChildKeys() {
    return materializedChildKeys;
  }

  private class MergeJoinIterable implements Iterable<Entity> {

    /**
     * The property on the parent entity that contains Keys
     * of the child entities.
     */
    private final String joinProperty;
    /**
     * Parent entities that meet all the parent criteria.
     */
    private final Iterable<Entity> parents;
    /**
     * Child entities that meet all the child criteria.
     */
    private final Iterator<Entity> childIter;

    private MergeJoinIterable(String joinProperty, Iterable<Entity> parents,
                              Iterator<Entity> childIter) {
      this.joinProperty = joinProperty;
      this.parents = parents;
      this.childIter = childIter;
    }

    public Iterator<Entity> iterator() {
      return new AbstractIterator<Entity>() {
        private final Iterator<Entity> parentEntityIter = parents.iterator();
        private Key curChildKey = null;
        protected Entity computeNext() {
          // We're going to iterate over all parents.
          // For each parent we're going to look at the value of the property
          // identified by joinProperty - these are child Keys.  If the child
          // Key is larger than the current child key from childIter we're
          // going to consume from childIter until we reach a Key that is
          // greater than or equal to the child Key on the parent.  If we
          // never reach this point, we're done.  If we do reach this point
          // we can then check to see if the child Key on the parent is in the
          // list of child Keys we've consumed and use that to determine if the
          // parent Entity belongs in the result set.
          while( parentEntityIter.hasNext()) {
            Entity curParentEntity = parentEntityIter.next();
            Object propertyValue = curParentEntity.getProperty(joinProperty);
            if (propertyValue == null) {
              // no key list so it can't be in the result set, move along
              continue;
            }
            List<?> curJoinKeyList;
            if (propertyValue instanceof Key) {
              curJoinKeyList = Utils.newArrayList(propertyValue);
            } else if (propertyValue instanceof List) {
              curJoinKeyList = (List<?>) propertyValue;
            } else {
              // not a Key and not a List, so it's not in the result set
              continue;
            }

            for (Object element : curJoinKeyList) {
              if (!(element instanceof Key)) {
                continue;
              }
              Key joinKey = (Key) element;
              // consume entities from childIter until curChildKey is smaller
              // than or equal to the joinKey
              while (curChildKey == null || joinKey.compareTo(curChildKey) > 0) {
                if (!childIter.hasNext()) {
                  break;
                }
                curChildKey = childIter.next().getKey();
                materializedChildKeys.add(curChildKey);
              }
              if (materializedChildKeys.contains(joinKey)) {
                return curParentEntity;
              }
            }
          }
          endOfData();
          return null;
        }
      };
    }
  }

  private static boolean sortAlreadyExists(String prop, Query.SortDirection dir, Query datastoreQuery) {
    for (Query.SortPredicate sp : datastoreQuery.getSortPredicates()) {
      if (sp.getPropertyName().equals(prop) && sp.getDirection() == dir) {
        return true;
      }
    }
    return false;
  }

}
