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
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Query.FilterPredicate;
import com.google.appengine.api.datastore.Query.SortPredicate;

import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.query.compiler.QueryCompilation;
import org.datanucleus.query.expression.OrderExpression;
import org.datanucleus.query.expression.VariableExpression;
import org.datanucleus.util.StringUtils;

import com.google.appengine.datanucleus.Utils;
import com.google.appengine.datanucleus.mapping.DatastoreTable;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Struct used to represent info about the query we need to fulfil.
 *
 * @author Max Ross <maxr@google.com>
 */
final class QueryData {
  final Map parameters;
  final AbstractClassMetaData acmd;
  final Map<String, DatastoreTable> tableMap = Utils.newHashMap();
  final QueryCompilation compilation;
  final Query primaryDatastoreQuery;
  final DatastoreQuery.ResultType resultType;
  final Utils.Function<Entity, Object> resultTransformer;
  final LinkedHashMap<String, List<Object>> inFilters = new LinkedHashMap<String, List<Object>>();
  Set<Key> batchGetKeys;

  // only used by JDO when there is an explicit variable
  VariableExpression joinVariableExpression;
  OrderExpression joinOrderExpression;
  Query joinQuery;
  String currentOrProperty;
  boolean isOrExpression = false;

  QueryData(
      Map parameters, AbstractClassMetaData acmd, DatastoreTable table,
      QueryCompilation compilation, Query primaryDatastoreQuery,
      DatastoreQuery.ResultType resultType,
      Utils.Function<Entity, Object> resultTransformer) {
    this.parameters = parameters;
    this.acmd = acmd;
    this.tableMap.put(acmd.getFullClassName(), table);
    this.compilation = compilation;
    this.primaryDatastoreQuery = primaryDatastoreQuery;
    this.resultType = resultType;
    this.resultTransformer = resultTransformer;
  }

  /**
   * Convenience method to return the datastore query to be invoked in String form (for logging).
   * @return The string form
   */
  public String getDatastoreQueryAsString() {
    StringBuilder str = new StringBuilder();
    str.append("Kind=" + StringUtils.collectionToString(tableMap.values()));
    List<FilterPredicate> filterPreds = primaryDatastoreQuery.getFilterPredicates();
    if (filterPreds.size() > 0) {
      str.append(" Filter : ");
      Iterator<FilterPredicate> filterIter = filterPreds.iterator();
      while (filterIter.hasNext()) {
        FilterPredicate pred = filterIter.next();
        str.append(pred.getPropertyName() + pred.getOperator() + pred.getValue());
        if (filterIter.hasNext()) {
          if (isOrExpression) {
            str.append(" OR ");
          } else {
            str.append(" AND ");
          }
        }
      }
    }

    List<SortPredicate> sortPreds = primaryDatastoreQuery.getSortPredicates();
    if (sortPreds.size() > 0) {
      str.append(" Order : ");
      Iterator<SortPredicate> sortIter = sortPreds.iterator();
      while (sortIter.hasNext()) {
        SortPredicate pred = sortIter.next();
        str.append(pred.getPropertyName() + " " + pred.getDirection());
        if (sortIter.hasNext()) {
          str.append(",");
        }
      }
    }
    return str.toString();
  }
}

