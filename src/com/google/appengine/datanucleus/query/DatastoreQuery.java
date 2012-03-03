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

import com.google.appengine.api.datastore.Cursor;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceConfig;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.QueryResultIterable;
import com.google.appengine.api.datastore.QueryResultIterator;
import com.google.appengine.api.datastore.QueryResultList;
import com.google.appengine.api.datastore.ReadPolicy;
import com.google.appengine.api.datastore.ShortBlob;
import com.google.appengine.api.datastore.Transaction;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.FetchPlan;
import org.datanucleus.PropertyNames;
import org.datanucleus.store.fieldmanager.FieldManager;
import org.datanucleus.api.ApiAdapter;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.exceptions.NucleusFatalUserException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.identity.OIDFactory;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.EmbeddedMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.MetaDataManager;
import org.datanucleus.query.compiler.QueryCompilation;
import org.datanucleus.query.expression.DyadicExpression;
import org.datanucleus.query.expression.Expression;
import org.datanucleus.query.expression.InvokeExpression;
import org.datanucleus.query.expression.JoinExpression;
import org.datanucleus.query.expression.Literal;
import org.datanucleus.query.expression.OrderExpression;
import org.datanucleus.query.expression.ParameterExpression;
import org.datanucleus.query.expression.PrimaryExpression;
import org.datanucleus.query.expression.VariableExpression;
import org.datanucleus.query.symbol.Symbol;
import org.datanucleus.query.symbol.SymbolTable;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.FieldValues;
import org.datanucleus.store.ObjectProvider;

import com.google.appengine.datanucleus.DatastoreExceptionTranslator;
import com.google.appengine.datanucleus.FetchFieldManager;
import com.google.appengine.datanucleus.DatastoreManager;
import com.google.appengine.datanucleus.DatastoreServiceFactoryInternal;
import com.google.appengine.datanucleus.DatastoreTransaction;
import com.google.appengine.datanucleus.EntityUtils;
import com.google.appengine.datanucleus.MetaDataUtils;
import com.google.appengine.datanucleus.PrimitiveArrays;
import com.google.appengine.datanucleus.Utils;
import com.google.appengine.datanucleus.Utils.Function;
import com.google.appengine.datanucleus.mapping.DatastoreTable;

import org.datanucleus.store.mapped.IdentifierFactory;
import org.datanucleus.store.mapped.mapping.EmbeddedMapping;
import org.datanucleus.store.mapped.mapping.JavaTypeMapping;
import org.datanucleus.store.mapped.mapping.PersistableMapping;
import org.datanucleus.store.query.AbstractJavaQuery;
import org.datanucleus.store.schema.naming.ColumnType;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.appengine.api.datastore.FetchOptions.Builder.withChunkSize;
import static com.google.appengine.api.datastore.FetchOptions.Builder.withLimit;
import static com.google.appengine.api.datastore.FetchOptions.Builder.withOffset;
import static com.google.appengine.api.datastore.FetchOptions.Builder.withStartCursor;

/**
 * A unified JDOQL/JPQL query implementation for Datastore.
 *
 * TODO Detect unsupported features and evaluate as much as possible in-datastore, and then
 * check flags for unsupported features and evaluate the rest in-memory.
 *
 * TODO(maxr): More logging
 * TODO(maxr): Localized exception messages.
 *
 * @author Erick Armbrust <earmbrust@google.com>
 * @author Max Ross <maxr@google.com>
 */
public class DatastoreQuery implements Serializable {

  // Exposed for testing
  static final Expression.Operator GROUP_BY_OP = new Expression.Operator("GROUP BY", Integer.MAX_VALUE);

  // Exposed for testing
  static final Expression.Operator HAVING_OP = new Expression.Operator("HAVING", Integer.MAX_VALUE);

  // Exposed for testing
  static final Expression.Operator JOIN_OP = new Expression.Operator("JOIN", Integer.MAX_VALUE);

  static final Set<Expression.Operator> UNSUPPORTED_OPERATORS =
      Utils.newHashSet((Expression.Operator) Expression.OP_ADD,
          (Expression.Operator) Expression.OP_COM,
          (Expression.Operator) Expression.OP_CONCAT,
          (Expression.Operator) Expression.OP_DIV,
          (Expression.Operator) Expression.OP_IS,
          (Expression.Operator) Expression.OP_ISNOT,
          (Expression.Operator) Expression.OP_LIKE,
          (Expression.Operator) Expression.OP_MOD,
          (Expression.Operator) Expression.OP_NEG,
          (Expression.Operator) Expression.OP_MUL,
          (Expression.Operator) Expression.OP_NOT,
          (Expression.Operator) Expression.OP_SUB);

  private static final Map<Expression.Operator, Query.FilterOperator> DATANUCLEUS_OP_TO_APPENGINE_OP = buildNewOpMap();

  private static Map<Expression.Operator, Query.FilterOperator> buildNewOpMap() {
    Map<Expression.Operator, Query.FilterOperator> map =
        new HashMap<Expression.Operator, Query.FilterOperator>();
    map.put(Expression.OP_EQ, Query.FilterOperator.EQUAL);
    map.put(Expression.OP_GT, Query.FilterOperator.GREATER_THAN);
    map.put(Expression.OP_GTEQ, Query.FilterOperator.GREATER_THAN_OR_EQUAL);
    map.put(Expression.OP_LT, Query.FilterOperator.LESS_THAN);
    map.put(Expression.OP_LTEQ, Query.FilterOperator.LESS_THAN_OR_EQUAL);
    map.put(Expression.OP_NOTEQ, Query.FilterOperator.NOT_EQUAL);
    return map;
  }

  /** Whether the caller will be evaluating any unsupported components in-memory when the datastore doesnt support. */
  boolean inmemoryWhenUnsupported = true;

  /** DataNucleus query that this is attempting to evaluate. */
  final AbstractJavaQuery query;

  /** Whether the filter clause is completely evaluatable in the datastore. */
  boolean filterComplete = true;

  /** Whether the order clause is completely evaluatable in the datastore. */
  boolean orderComplete = true;

  /** The different types of datastore query results that we support. */
  enum ResultType {
    ENTITY, // return entities
    KEYS_ONLY // return just the keys
  }

  /**
   * Constructs a new GAE query based on a DataNucleus query.
   * @param query The DataNucleus query to be translated into a GAE query.
   */
  public DatastoreQuery(AbstractJavaQuery query) {
    this.query = query;
  }

  public boolean isFilterComplete() {
      return filterComplete;
  }

  public boolean isOrderComplete() {
      return orderComplete;
  }

  /**
   * Method to compile the query into a GAE Query.
   * @param compilation The compiled query.
   * @param parameters Parameter values for the query.
   * @param inmemoryWhenUnsupported Whether we will be evaluating in-memory when not supported in the datastore
   * @return The QueryData 'compilation' for this query
   */
  public QueryData compile(QueryCompilation compilation, Map<String, ?> parameters, boolean inmemoryWhenUnsupported) {
    this.inmemoryWhenUnsupported = inmemoryWhenUnsupported;

    if (query.getCandidateClass() == null) {
      throw new NucleusFatalUserException(
          "Candidate class could not be found: " + query.getSingleStringQuery());
    }

    final ClassLoaderResolver clr = getClassLoaderResolver();
    final AbstractClassMetaData acmd = getMetaDataManager().getMetaDataForClass(query.getCandidateClass(), clr);
    if (acmd == null) {
      throw new NucleusFatalUserException("No meta data for " + query.getCandidateClass().getName()
          + ".  Perhaps you need to run the enhancer on this class?");
    }
    getStoreManager().validateMetaDataForClass(acmd);

    if (compilation.getSubqueryAliases() != null && compilation.getSubqueryAliases().length > 0) {
      throw new NucleusUserException("Subqueries not supported by datastore. Try evaluating them in-memory");
    }

    // Create QueryData object to use as the datastore compilation
    ResultType resultType = validateResultExpression(compilation, acmd);
    Function<Entity, Object> resultTransformer;
    if (resultType == ResultType.KEYS_ONLY) {
      resultTransformer = new Function<Entity, Object>() {
        public Object apply(Entity from) {
          return entityToPojoPrimaryKey(from, acmd, clr, getExecutionContext());
        }
      };
    } else {
      resultTransformer = new Function<Entity, Object>() {
        public Object apply(Entity from) {
          FetchPlan fp = query.getFetchPlan();
          return EntityUtils.entityToPojo(from, acmd, clr, getExecutionContext(), query.getIgnoreCache(), fp);
        }
      };
    }

    DatastoreTable table = getStoreManager().getDatastoreClass(acmd.getFullClassName(), clr);
    String kind = table.getIdentifier().getIdentifierName();
    QueryData qd = new QueryData(parameters, acmd, table, compilation, new Query(kind), resultType, 
        resultTransformer);

    if (compilation.getExprFrom() != null) {
      // Process FROM expression, adding pseudo joins
      for (Expression fromExpr : compilation.getExprFrom()) {
        processFromExpression(qd, fromExpr);
      }
    }

    addDiscriminator(qd, clr);
    addMultitenancyDiscriminator(qd);
    addFilters(qd);
    addSorts(qd);

    return qd;
  }

  /**
   * Method to execute the query implied by specified QueryData object.
   * @param qd QueryData to be executed
   * @return The result of executing the query.
   */
  public Object performExecute(QueryData qd) {

    // Obtain DatastoreService
    DatastoreServiceConfig config = getStoreManager().getDefaultDatastoreServiceConfigForReads();
    if (query.getDatastoreReadTimeoutMillis() > 0) {
      // config wants the timeout in seconds
      config.deadline(query.getDatastoreReadTimeoutMillis() / 1000);
    }
    Map extensions = query.getExtensions();
    if (extensions != null && extensions.get(DatastoreManager.DATASTORE_READ_CONSISTENCY_PROPERTY) != null) {
      config.readPolicy(new ReadPolicy(
          ReadPolicy.Consistency.valueOf((String) extensions.get(DatastoreManager.DATASTORE_READ_CONSISTENCY_PROPERTY))));
    }
    DatastoreService ds = DatastoreServiceFactoryInternal.getDatastoreService(config);

    // Execute the most appropriate type of query
    if (qd.batchGetKeys != null &&
        qd.primaryDatastoreQuery.getFilterPredicates().size() == 1 &&
        qd.primaryDatastoreQuery.getSortPredicates().isEmpty()) {
      // Batch Get Query - only execute a batch get if there aren't any other filters or sorts
      return executeBatchGetQuery(ds, qd);
    } else if (qd.joinQuery != null) {
      // Join Query
      FetchOptions opts = buildFetchOptions(query.getRangeFromIncl(), query.getRangeToExcl());
      return wrapEntityQueryResult(new JoinHelper().executeJoinQuery(qd, this, ds, opts), qd.resultTransformer, ds, null);
    } else {
      // Normal Query
      return executeNormalQuery(ds, qd);
    }
  }

  private Object executeNormalQuery(DatastoreService ds, QueryData qd) {
    latestDatastoreQuery = qd.primaryDatastoreQuery;
    Transaction txn = null;
    // give users a chance to opt-out of having their query execute in a txn
    Map extensions = query.getExtensions();
    if (extensions == null ||
        !extensions.containsKey(DatastoreManager.QUERYEXT_EXCLUDE_FROM_TXN) ||
        !(Boolean)extensions.get(DatastoreManager.QUERYEXT_EXCLUDE_FROM_TXN)) {
      // If this is an ancestor query, execute it in the current transaction
      txn = qd.primaryDatastoreQuery.getAncestor() != null ? ds.getCurrentTransaction(null) : null;
    }

    PreparedQuery preparedQuery = ds.prepare(txn, qd.primaryDatastoreQuery);
    FetchOptions opts = buildFetchOptions(query.getRangeFromIncl(), query.getRangeToExcl());

    if (qd.resultType == ResultType.KEYS_ONLY || isBulkDelete()) {
      qd.primaryDatastoreQuery.setKeysOnly();
    }

    if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled()) {
      NucleusLogger.DATASTORE_NATIVE.debug("Executing query in datastore for " + query.toString());
    }

    Iterable<Entity> entityIterable;
    Cursor endCursor = null;
    if (opts != null) {
      if (opts.getLimit() != null) {
        QueryResultList<Entity> entities = preparedQuery.asQueryResultList(opts);
        endCursor = entities.getCursor();
        entityIterable = entities;
      } else {
        entityIterable = preparedQuery.asQueryResultIterable(opts);
      }
    } else {
      entityIterable = preparedQuery.asQueryResultIterable();
    }

    return wrapEntityQueryResult(entityIterable, qd.resultTransformer, ds, endCursor);
  }

  private Object executeBatchGetQuery(DatastoreService ds, QueryData qd) {
    DatastoreTransaction txn = getStoreManager().getDatastoreTransaction(getExecutionContext());
    Transaction innerTxn = txn == null ? null : txn.getInnerTxn();
    if (isBulkDelete()) {
      Set<Key> keysToDelete = qd.batchGetKeys;
      Map extensions = query.getExtensions();
      if (extensions != null &&
          extensions.containsKey(DatastoreManager.QUERYEXT_SLOW_BUT_MORE_ACCURATE_JPQL_DELETE) &&
          (Boolean) extensions.get(DatastoreManager.QUERYEXT_SLOW_BUT_MORE_ACCURATE_JPQL_DELETE)) {
        Map<Key, Entity> getResult = ds.get(innerTxn, qd.batchGetKeys);
        keysToDelete = getResult.keySet();
      }

      // The datastore doesn't give any indication of how many entities were
      // actually deleted, so by default we just return the number of keys
      // that we were asked to delete.  If the "slow-but-more-accurate" extension
      // is set for the query we'll first fetch the entities identified by the
      // keys and then delete whatever is returned.  This is more accurate but
      // not guaranteed accurate, since if we're executing without a txn,
      // something could get deleted in between the fetch and the delete.
      if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled()) {
        NucleusLogger.DATASTORE_NATIVE.debug("Deleting entities with keys " + StringUtils.collectionToString(keysToDelete));
      }
      ds.delete(innerTxn, keysToDelete);

      return (long) keysToDelete.size();
    } else {
      Map<Key, Entity> entityMap = ds.get(innerTxn, qd.batchGetKeys);
      // return the entities in the order in which the keys were provided
      Collection<Entity> entities = new ArrayList<Entity>();
      for (Key key : qd.batchGetKeys) {
        Entity entity = entityMap.get(key);
        if (entity != null) {
          entities.add(entity);
        }
      }

      return newStreamingQueryResultForEntities(entities, qd.resultTransformer, null, query);
    }
  }

  private Object wrapEntityQueryResult(Iterable<Entity> entities, Function<Entity, Object> resultTransformer,
      DatastoreService ds, Cursor endCursor) {
    if (isBulkDelete()) {
      return deleteEntityQueryResult(entities, ds);
    }
    return newStreamingQueryResultForEntities(entities, resultTransformer, endCursor, query);
  }

  private long deleteEntityQueryResult(Iterable<Entity> entities, DatastoreService ds) {
    List<Key> keysToDelete = Utils.newArrayList();
    for (Entity e : entities) {
      keysToDelete.add(e.getKey());
    }

    if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled()) {
      NucleusLogger.DATASTORE_NATIVE.debug("Deleting entities with keys " + StringUtils.collectionToString(keysToDelete));
    }
    ds.delete(ds.getCurrentTransaction(null), keysToDelete);

    return (long) keysToDelete.size();
  }

  public static List<?> newStreamingQueryResultForEntities(
      Iterable<Entity> entities, final Function<Entity, Object> resultTransformer,
      Cursor endCursor, AbstractJavaQuery query) {
    final RuntimeExceptionWrappingIterable iterable;
    final ApiAdapter api = query.getExecutionContext().getApiAdapter();
    if (entities instanceof QueryResultIterable) {
      // need to wrap it in a specialization so that CursorHelper can reach in
      iterable = new RuntimeExceptionWrappingIterable(api, (QueryResultIterable<Entity>) entities) {
        @Override
        Iterator<Entity> newIterator(Iterator<Entity> innerIter) {
          return new RuntimeExceptionWrappingQueryResultIterator(api, this, 
              (QueryResultIterator<Entity>) innerIter);
        }
      };
    } else {
      iterable = new RuntimeExceptionWrappingIterable(api, entities);
    }

    return new StreamingQueryResult(query, iterable, resultTransformer, endCursor);
  }

  /**
   * Build a FetchOptions instance using the provided params.
   * @return A FetchOptions instance built using the provided params, or {@code null} if neither param is set.
   */
  FetchOptions buildFetchOptions(long fromInclNo, long toExclNo) {
    FetchOptions opts = null;
    Integer offset = null;
    if (fromInclNo != 0 && fromInclNo != Long.MAX_VALUE) {
      // datastore api expects an int because we cap you at 1000 anyway.
      offset = (int) Math.min(Integer.MAX_VALUE, fromInclNo);
      opts = withOffset(offset);
    }

    if (toExclNo != Long.MAX_VALUE) {
      // datastore api expects an int because we cap you at 1000 anyway.
      int intExclNo = (int) Math.min(Integer.MAX_VALUE, toExclNo);
      if (opts == null) {
        // When fromInclNo isn't specified, intExclNo (the index of the last
        // result to return) and limit are the same.
        opts = withLimit(intExclNo);
      } else {
        // When we have values for both fromInclNo and toExclNo
        // we can't take toExclNo as the limit for the query because
        // toExclNo is the index of the last result, not the max
        // results to return.  In this scenario the limit is the
        // index of the last result minus the offset.  For example, if
        // fromInclNo is 10 and toExclNo is 25, the limit for the query
        // is 15 because we want 15 results starting after the first 10.

        // We know that offset won't be null because opts is not null.
        opts.limit(intExclNo - offset);
      }
    }

    // users can provide the cursor as a Cursor or its String representation.
    // If we have a cursor, add it to the fetch options
    Cursor cursor = null;
    Object obj = query.getExtension(CursorHelper.QUERY_CURSOR_PROPERTY_NAME);
    if (obj != null) {
      if (obj instanceof Cursor) {
        cursor = (Cursor) obj;
      }
      else {
        cursor = Cursor.fromWebSafeString((String) obj);
      }
    }
    if (cursor != null) {
      if (opts == null) {
        opts = withStartCursor(cursor);
      } else {
        opts.startCursor(cursor);
      }
    }

    // Use the fetch size of the fetch plan to determine chunk size.
    FetchPlan fetchPlan = query.getFetchPlan();
    Integer fetchSize = fetchPlan.getFetchSize();
    if (fetchSize != FetchPlan.FETCH_SIZE_OPTIMAL) {
      if (fetchSize == FetchPlan.FETCH_SIZE_GREEDY) {
        fetchSize = Integer.MAX_VALUE;
      }
    } else {
      fetchSize = null;
    }
    if (fetchSize != null) {
      if (opts == null) {
        opts = withChunkSize(fetchSize);
      } else {
        opts.chunkSize(fetchSize);
      }
    }

    return opts;
  }

  /**
   * Converts the provided entity to its pojo primary key representation.
   * @param entity The entity to convert
   * @param acmd The meta data for the pojo class
   * @param clr The classloader resolver
   * @param ec The executionContext
   * @return The pojo that corresponds to the id of the provided entity.
   */
  private static Object entityToPojoPrimaryKey(final Entity entity, final AbstractClassMetaData acmd,
      ClassLoaderResolver clr, ExecutionContext ec) {

    FieldValues fv = new FieldValues() {
      public void fetchFields(ObjectProvider op) {
        op.replaceFields(acmd.getPKMemberPositions(), new FetchFieldManager(op, entity));
      }
      public void fetchNonLoadedFields(ObjectProvider op) {}
      public FetchPlan getFetchPlanForLoading() {
        return null;
      }
    };

    Object id = null;
    Class cls = clr.classForName(acmd.getFullClassName());
    if (acmd.getIdentityType() == IdentityType.APPLICATION) {
      FieldManager fm = new QueryEntityPKFetchFieldManager(acmd, entity);
      id = IdentityUtils.getApplicationIdentityForResultSetRow(ec, acmd, cls, true, fm);
    }
    else if (acmd.getIdentityType() == IdentityType.DATASTORE) {
      Key key = entity.getKey();
      if (key.getName() != null) {
        // String based
        id = OIDFactory.getInstance(ec.getNucleusContext(), key.getName());
      } else {
        // Numeric based
        id = OIDFactory.getInstance(ec.getNucleusContext(), key.getId());
      }
    }

    return ec.findObject(id, fv, cls, false);
  }

  /**
   * Process the result expression and return the result type needed for that.
   * @param compilation The compiled query
   * @param acmd The meta data for the class we're querying
   * @return The ResultType
   */
  private ResultType validateResultExpression(QueryCompilation compilation, AbstractClassMetaData acmd) {

    if (compilation.getExprResult() == null) {
      return ResultType.ENTITY;
    }

    boolean keysOnly = true;
    for (Expression resultExpr : compilation.getExprResult()) {
      if (resultExpr instanceof InvokeExpression) {
        InvokeExpression invokeExpr = (InvokeExpression) resultExpr;
        if (invokeExpr.getOperation().toLowerCase().equals("count")) {
          // Only need key for this
        } else {
          // May need non-key for this
          keysOnly = false;
        }
      } else if (resultExpr instanceof PrimaryExpression) {
        PrimaryExpression primaryExpr = (PrimaryExpression) resultExpr;
        if (!primaryExpr.getId().equals(compilation.getCandidateAlias())) {
          AbstractMemberMetaData ammd =
            getMemberMetaDataForTuples(acmd, getTuples(primaryExpr, compilation.getCandidateAlias()));
          if (ammd == null) {
            throw noMetaDataException(primaryExpr.getId(), acmd.getFullClassName());
          }
          if (!ammd.isPrimaryKey()) {
            keysOnly = false;
          }
        }
      } else {
        // May need non-key fields
        keysOnly = false;
      }
    }

    return (keysOnly ? ResultType.KEYS_ONLY : ResultType.ENTITY);
  }

  private void processFromExpression(QueryData qd, Expression expr) {
    if (expr instanceof JoinExpression) {
      JoinExpression joinExpr = (JoinExpression) expr;
      if (joinExpr.getType() != JoinExpression.JoinType.JOIN_INNER &&
          joinExpr.getType() != JoinExpression.JoinType.JOIN_INNER_FETCH) {
        throw new UnsupportedDatastoreFeatureException("Cannot fulfill outer join queries.");
      }
      qd.joinOrderExpression = createJoinOrderExpression(joinExpr.getPrimaryExpression());
    }
    if (expr.getLeft() != null) {
      processFromExpression(qd, expr.getLeft());
    }
    if (expr.getRight() != null) {
      processFromExpression(qd, expr.getRight());
    }
  }

  /**
   * Adds sorts to the given {@link Query} by examining the compiled order expression.
   */
  private void addSorts(QueryData qd) {
    Expression[] orderBys = qd.compilation.getExprOrdering();
    if (orderBys == null) {
      return;
    }

    try {
      for (Expression expr : orderBys) {
        OrderExpression orderExpr = (OrderExpression) expr;
        Query.SortDirection dir = getSortDirection(orderExpr);
        String sortProp = getSortProperty(qd, orderExpr);
        qd.primaryDatastoreQuery.addSort(sortProp, dir);
      }
    } catch (NucleusException ne) {
      if (inmemoryWhenUnsupported) {
        orderComplete = false;
      } else {
        throw ne;
      }
    }
  }

  private static Query.SortDirection getSortDirection(OrderExpression oe) {
    return oe.getSortOrder() == null || oe.getSortOrder().equals("ascending")
            ? Query.SortDirection.ASCENDING : Query.SortDirection.DESCENDING;
  }

  /**
   * @param qd The QueryData
   * @param orderExpr The OrderExpression
   * @return The name of the sort property that was added to the primary datastore query.
   */
  String getSortProperty(QueryData qd, OrderExpression orderExpr) {
    PrimaryExpression left = (PrimaryExpression) orderExpr.getLeft();
    AbstractClassMetaData acmd = qd.acmd;
    List<String> tuples = getTuples(left, qd.compilation.getCandidateAlias());
    if (isJoin(left.getLeft(), tuples)) {
      // Change the class meta data to the meta-data for the joined class
      acmd = getJoinClassMetaData(left.getLeft(), tuples, qd);
    }

    AbstractMemberMetaData ammd = getMemberMetaDataForTuples(acmd, tuples);
    if (ammd == null) {
      throw noMetaDataException(left.getId(), acmd.getFullClassName());
    }
    if (MetaDataUtils.isParentPKField(ammd)) {
      throw new UnsupportedDatastoreFeatureException("Cannot sort by parent.");
    } else {
      String sortProp;
      if (ammd.isPrimaryKey()) {
        sortProp = Entity.KEY_RESERVED_PROPERTY;
      } else {
        sortProp = determinePropertyName(ammd);
      }
      return sortProp;
    }
  }

  private boolean isJoin(Expression expr, List<String> tuples) {
    return expr instanceof VariableExpression ||
        (tuples.size() > 1 && getSymbolTable().hasSymbol(tuples.get(0)));
  }

  /**
   * Method to add a filter to restrict any discriminator property to valid values.
   * @param qd QueryData
   * @param clr ClassLoader resolver
   */
  private void addDiscriminator(QueryData qd, ClassLoaderResolver clr) {
    if (qd.acmd.hasDiscriminatorStrategy()) {
      String className = qd.acmd.getFullClassName();
      boolean includeSubclasses = query.isSubclasses();
      DatastoreManager storeMgr = getStoreManager();
      String discriminatorPropertyName = EntityUtils.getDiscriminatorPropertyName(storeMgr.getIdentifierFactory(), 
          qd.acmd.getDiscriminatorMetaDataRoot());

      // Note : we always restrict the discriminator since the user may at some later point add other classes
      // to be persisted here, or have others that have data but aren't currently active in the persistence process
      List<Object> discriminatorValues = new ArrayList<Object>();
      discriminatorValues.add(qd.acmd.getDiscriminatorValue());
      if (includeSubclasses) {
        for (String subClassName : storeMgr.getSubClassesForClass(className, true, clr)) {
          AbstractClassMetaData subCmd = storeMgr.getMetaDataManager().getMetaDataForClass(subClassName, clr);
          discriminatorValues.add(subCmd.getDiscriminatorValue());
        }
      }

      qd.primaryDatastoreQuery.addFilter(discriminatorPropertyName, Query.FilterOperator.IN, discriminatorValues);
    }
  }

  /**
   * Method to add a filter to restrict any multitenancy discriminator to a valid value.
   * @param qd QueryData
   */
  private void addMultitenancyDiscriminator(QueryData qd) {
    if (getStoreManager().getStringProperty(PropertyNames.PROPERTY_TENANT_ID) != null) {
      if ("true".equalsIgnoreCase(qd.acmd.getValueForExtension("multitenancy-disable"))) {
      } else {
        // Restrict to the current tenant
        String multitenantPropName = getStoreManager().getNamingFactory().getColumnName(qd.acmd, ColumnType.MULTITENANCY_COLUMN);
        qd.primaryDatastoreQuery.addFilter(multitenantPropName, Query.FilterOperator.EQUAL, 
            getStoreManager().getStringProperty(PropertyNames.PROPERTY_TENANT_ID));
      }
    }
  }

  /**
   * Adds filters to the given {@link Query} by examining the compiled filter expression.
   */
  private void addFilters(QueryData qd) {
    try {
      addExpression(qd.compilation.getExprFilter(), qd);

      if (!qd.inFilters.isEmpty()) {
        boolean onlyKeyFilters = true;
        Set<Key> batchGetKeys = Utils.newLinkedHashSet();
        for (Map.Entry<String, List<Object>> entry : qd.inFilters.entrySet()) {
          if (!entry.getKey().equals(Entity.KEY_RESERVED_PROPERTY)) {
            onlyKeyFilters = false;
          } else {
            for (Object obj : entry.getValue()) {
              // Add to our list of batch get keys in case all the in filters
              // end up being on the primary key
              batchGetKeys.add(internalPkToKey(qd.acmd, obj));
            }
          }
          qd.primaryDatastoreQuery.addFilter(entry.getKey(), Query.FilterOperator.IN, entry.getValue());
        }

        if (onlyKeyFilters) {
          // All the in filters were on key so convert this to a batch get
          if (qd.batchGetKeys == null) {
            qd.batchGetKeys = batchGetKeys;
          } else {
            qd.batchGetKeys.addAll(batchGetKeys);
          }
        }
      }
    } catch (NucleusException ne) {
      if (!inmemoryWhenUnsupported || qd.isOrExpression) { // TODO Handle OR expressions in block below and get all candidates
        throw ne;
      }
      if (qd.isOrExpression) {
        // TODO We can't process something and an OR is involved, so just remove all filters
      }
      filterComplete = false;
    }
  }

  /**
   * Recursively walks the given expression, adding filters to the given {@link Query} where appropriate.
   * @throws UnsupportedDatastoreOperatorException If we encounter an operator that we don't support.
   * @throws UnsupportedDatastoreFeatureException If the query uses a feature that we don't support.
   */
  private void addExpression(Expression expr, QueryData qd) {
    if (expr == null) {
      return;
    }
    if (UNSUPPORTED_OPERATORS.contains(expr.getOperator())) {
      throw new UnsupportedDatastoreOperatorException(query.getSingleStringQuery(), expr.getOperator());
    }
    if (qd.isOrExpression) {
      if (expr.getOperator() != null && !expr.getOperator().equals(Expression.OP_EQ) && 
          !expr.getOperator().equals(Expression.OP_OR)) {
        throw new UnsupportedDatastoreFeatureException("'or' filters can only check equality");
      }
    }

    if (expr instanceof DyadicExpression) {
      if (expr.getOperator().equals(Expression.OP_AND)) {
        addExpression(expr.getLeft(), qd);
        addExpression(expr.getRight(), qd);
      } else if (expr.getOperator().equals(Expression.OP_OR)) {
        boolean reset = !qd.isOrExpression;
        qd.isOrExpression = true;
        addExpression(expr.getLeft(), qd);
        addExpression(expr.getRight(), qd);
        // we could have OR(OR(EQ(P1, 'yar'), EQ(P1, 'yar2'))
        // so only reset if it wasn't an or expression when we entered
        if (reset) {
          qd.isOrExpression = false;
          qd.currentOrProperty = null;
        }
      } else if (DATANUCLEUS_OP_TO_APPENGINE_OP.get(expr.getOperator()) == null) {
        throw new UnsupportedDatastoreOperatorException(query.getSingleStringQuery(),
            expr.getOperator());
      } else if (expr.getLeft() instanceof PrimaryExpression) {
        addLeftPrimaryExpression(
            (PrimaryExpression) expr.getLeft(), expr.getOperator(), expr.getRight(), qd);
      } else {
        // Recurse!
        addExpression(expr.getLeft(), qd);
        addExpression(expr.getRight(), qd);
      }
    } else if (expr instanceof PrimaryExpression) {
      // Recurse!
      addExpression(expr.getLeft(), qd);
      addExpression(expr.getRight(), qd);
    } else if (expr instanceof InvokeExpression) {
      // InvokeExpression that return boolean
      InvokeExpression invokeExpr = ((InvokeExpression) expr);
      if (invokeExpr.getOperation().equals("contains") && invokeExpr.getArguments().size() == 1) {
        handleContainsOperation(invokeExpr, qd);
      }else if (invokeExpr.getOperation().equals("startsWith") && invokeExpr.getArguments().size() == 1) {
        handleStartsWithOperation(invokeExpr, qd);
      } else if (invokeExpr.getOperation().equals("matches")) {
        handleMatchesOperation(invokeExpr, qd);
      } else {
        throw newUnsupportedQueryMethodException(invokeExpr);
      }
    } else if (expr instanceof VariableExpression) {
      // We don't support variables
      VariableExpression varExpr = (VariableExpression) expr;
      throw new NucleusFatalUserException(
          "Unexpected expression type while parsing query. Variables not supported by GAE (" + varExpr.getId() + ")");
    } else {
      throw new UnsupportedDatastoreFeatureException(
          "Unexpected expression type while parsing query: "+ expr.getClass().getName());
    }
  }

  private void handleMatchesOperation(InvokeExpression invokeExpr, QueryData qd) {
    Expression param = (Expression) invokeExpr.getArguments().get(0);
    Expression escapeParam = null;
    if (invokeExpr.getArguments().size() == 2) {
      escapeParam = invokeExpr.getArguments().get(1);
      // TODO Support escape syntax
      throw new UnsupportedDatastoreFeatureException("GAE doesn't currently support ESCAPE syntax (" + escapeParam + ")");
    }

    if (invokeExpr.getLeft() instanceof PrimaryExpression) {
      PrimaryExpression leftExpr = (PrimaryExpression)invokeExpr.getLeft();

      // Make sure that the left expression is a String
      List<String> tuples = getTuples(leftExpr, qd.compilation.getCandidateAlias());
      if (tuples.size() == 1) {
        // Handle case of simple field name
        AbstractMemberMetaData mmd = qd.acmd.getMetaDataForMember(tuples.get(0));
        if (mmd != null && !String.class.isAssignableFrom(mmd.getType())) {
          throw new UnsupportedDatastoreFeatureException("The 'matches' method is only for use with a String expression");
        }
      }

      if (param instanceof Literal) {
        String matchesExpr = getPrefixFromMatchesExpression(((Literal) param).getLiteral());
        addPrefix(leftExpr, new Literal(matchesExpr), matchesExpr, qd);
        return;
      } else if (param instanceof ParameterExpression) {
        ParameterExpression parameterExpression = (ParameterExpression) param;
        Object parameterValue = getParameterValue(qd.parameters, parameterExpression);
        String matchesExpr = getPrefixFromMatchesExpression(parameterValue);
        addPrefix(leftExpr, new Literal(matchesExpr), matchesExpr, qd);
        return;
      }
    }

    // We don't know what this is.
    throw newUnsupportedQueryMethodException(invokeExpr);
  }

  private String getPrefixFromMatchesExpression(Object matchesExprObj) {
    if (matchesExprObj instanceof Character) {
      matchesExprObj = matchesExprObj.toString();
    }
    if (!(matchesExprObj instanceof String)) {
      throw new NucleusFatalUserException(
          "Prefix matching only supported on strings (received a "
          + matchesExprObj.getClass().getName() + ").");
    }
    String matchesExpr = (String) matchesExprObj;
    String wildcardExpr = getWildcardExpression();
    int wildcardIndex = matchesExpr.indexOf(wildcardExpr);
    if (wildcardIndex == -1 || wildcardIndex != matchesExpr.length() - wildcardExpr.length()) {
      throw new UnsupportedDatastoreFeatureException(
          "Wildcard must appear at the end of the expression string (only prefix matches are supported)");
    }
    return matchesExpr.substring(0, wildcardIndex);
  }

  private String getWildcardExpression() {
    if (getStoreManager().getApiAdapter().getName().equalsIgnoreCase("JPA")) {
      return "%";
    }
    return ".*";
  }

  private void addPrefix(PrimaryExpression left, Expression right, String prefix, QueryData qd) {
    addLeftPrimaryExpression(left, Expression.OP_GTEQ, right, qd);
    Expression param = getUpperLimitForStartsWithStr(prefix);
    addLeftPrimaryExpression(left, Expression.OP_LT, param, qd);
  }

  /**
   * We fulfill startsWith by adding a >= filter for the method argument and a
   * < filter for the method argument translated into an upper limit for the scan.
   */
  private void handleStartsWithOperation(InvokeExpression invokeExpr, QueryData qd) {
    Expression param = (Expression) invokeExpr.getArguments().get(0);
    param.bind(getSymbolTable());

    if (invokeExpr.getLeft() instanceof PrimaryExpression) {
      PrimaryExpression left = (PrimaryExpression) invokeExpr.getLeft();

      // Make sure that the left expression is a String
      List<String> tuples = getTuples(left, qd.compilation.getCandidateAlias());
      if (tuples.size() == 1) {
        // Handle case of simple field name
        AbstractMemberMetaData mmd = qd.acmd.getMetaDataForMember(tuples.get(0));
        if (mmd != null && !String.class.isAssignableFrom(mmd.getType())) {
          throw new UnsupportedDatastoreFeatureException("The 'startsWith' method is only for use with a String expression");
        }
      }

      if (param instanceof Literal) {
        addPrefix(left, param, (String) ((Literal) param).getLiteral(), qd);
        return;
      } else if (param instanceof ParameterExpression) {
        Object parameterValue = getParameterValue(qd.parameters, (ParameterExpression) param);
        addPrefix(left, param, (String) parameterValue, qd);
        return;
      }
    }

    // Unsupported combination
    throw newUnsupportedQueryMethodException(invokeExpr);
  }

  private void handleContainsOperation(InvokeExpression invokeExpr, QueryData qd) {
    Expression param = (Expression) invokeExpr.getArguments().get(0);
    param.bind(getSymbolTable());

    if (invokeExpr.getLeft() instanceof PrimaryExpression) {
      PrimaryExpression left = (PrimaryExpression) invokeExpr.getLeft();

      // Make sure that the left expression is a collection
      List<String> tuples = getTuples(left, qd.compilation.getCandidateAlias());
      if (tuples.size() == 1) {
        // Handle case of simple field name
        AbstractMemberMetaData mmd = qd.acmd.getMetaDataForMember(tuples.get(0));
        if (mmd != null && !Collection.class.isAssignableFrom(mmd.getType())) {
          throw new UnsupportedDatastoreFeatureException("The 'contains' method is only for use with a Collection expression");
        }
      }

      // treat contains as equality since that's how the low-level api does checks on multi-value properties.
      addLeftPrimaryExpression(left, Expression.OP_EQ, param, qd);
    } else if (invokeExpr.getLeft() instanceof ParameterExpression && param instanceof PrimaryExpression) {
      ParameterExpression pe = (ParameterExpression) invokeExpr.getLeft();
      addLeftPrimaryExpression((PrimaryExpression) param, Expression.OP_EQ, pe, qd);
    } else {
      throw newUnsupportedQueryMethodException(invokeExpr);
    }
  }

  /**
   * Converts a string like "ya" to "yb", but does so at the byte level to
   * model the actual behavior of the datastore.
   */
  private Literal getUpperLimitForStartsWithStr(String val) {
    byte[] bytes = val.getBytes();
    for (int i = bytes.length - 1; i >= 0; i--) {
      byte[] endKey = new byte[i + 1];
      System.arraycopy(bytes, 0, endKey, 0, i + 1);
      if (++endKey[i] != 0) {
        return new Literal(new String(endKey));
      }
    }
    return null;
  }

  private UnsupportedDatastoreFeatureException newUnsupportedQueryMethodException(
      InvokeExpression invocation) {
    throw new UnsupportedDatastoreFeatureException(
        "Unsupported method <" + invocation.getOperation() + "> while parsing expression: " + invocation);
  }

  /**
   * Accessor for parameter value for the provided parameter expression.
   * @param qd QueryData
   * @param pe Expression for the parameter
   * @return The value of this parameter
   */
  private static Object getParameterValue(Map paramValues, ParameterExpression pe) {
    Object key = null;
    if (paramValues.containsKey(pe.getId())) {
      key = pe.getId();
    } else {
      try {
        Integer intVal = Integer.valueOf(pe.getId());
        if (paramValues.containsKey(intVal)) {
          key = intVal;
        }
      } catch (NumberFormatException nfe) {}

      if (key == null) {
        key = pe.getPosition();
      }
    }
    return paramValues.get(key);
  }

  private void addLeftPrimaryExpression(PrimaryExpression left,
      Expression.Operator operator, Expression right, QueryData qd) {
    Query.FilterOperator op = DATANUCLEUS_OP_TO_APPENGINE_OP.get(operator);
    if (op == null) {
      throw new UnsupportedDatastoreFeatureException("Operator " + operator + " does not have a "
          + "corresponding operator in the datastore api.");
    }
    Object value;
    if (right instanceof PrimaryExpression) {
      value = qd.parameters.get(((PrimaryExpression) right).getId());
    } else if (right instanceof Literal) {
      value = ((Literal) right).getLiteral();
    } else if (right instanceof ParameterExpression) {
      value = getParameterValue(qd.parameters, (ParameterExpression) right);
    } else if (right instanceof DyadicExpression) {
      value = getValueFromDyadicExpression(right);
    } else if (right instanceof InvokeExpression) {
      InvokeExpression invoke = (InvokeExpression) right;
      // can't support CURRENT_TIME because we don't have a Time meaning.
      // maybe we can store Time fields as int64 without the temporal meaning?
      if (invoke.getOperation().equals("CURRENT_TIMESTAMP") ||
          invoke.getOperation().equals("CURRENT_DATE")) {
        value = NOW_PROVIDER.now();
      } else {
        // We don't support any other InvokeExpressions right now but we can at least give a better error.
        throw newUnsupportedQueryMethodException((InvokeExpression) right);
      }
    } else if (right instanceof VariableExpression) {
      // assume the variable is for a join
      if (!op.equals(Query.FilterOperator.EQUAL)) {
        throw new UnsupportedDatastoreFeatureException("Operator " + operator + " cannot be "
            + "used as part of the join condition.  Use 'contains' if joining on a Collection field "
            + "and equality if joining on a single-value field.");
      }

      // add an ordering on the column that we'll add in later.
      qd.joinVariableExpression = (VariableExpression) right;
      qd.joinOrderExpression = createJoinOrderExpression(left);
      return;
    } else {
      throw new UnsupportedDatastoreFeatureException(
          "Right side of expression is of unexpected type: " + right.getClass().getName());
    }
    List<String> tuples = getTuples(left, qd.compilation.getCandidateAlias());
    AbstractClassMetaData acmd = qd.acmd;
    Query datastoreQuery = qd.primaryDatastoreQuery;
    if (isJoin(left.getLeft(), tuples)) {
      acmd = getJoinClassMetaData(left.getLeft(), tuples, qd);
      // Get the query we're building up for the join
      datastoreQuery = qd.joinQuery;
      if (datastoreQuery == null) {
        // Query doesn't exist so create it
        String kind = EntityUtils.determineKind(acmd, getExecutionContext());
        datastoreQuery = new Query(kind);
        datastoreQuery.setKeysOnly();
        qd.joinQuery = datastoreQuery;
      }
    }
    AbstractMemberMetaData ammd = getMemberMetaDataForTuples(acmd, tuples);
    if (ammd == null) {
      throw noMetaDataException(left.getId(), acmd.getFullClassName());
    }
    JavaTypeMapping mapping = getMappingForFieldWithName(tuples, qd, acmd);
    if (mapping instanceof PersistableMapping) {
      processPersistableMember(qd, op, ammd, value);
    } else if (MetaDataUtils.isParentPKField(ammd)) {
      addParentFilter(op, internalPkToKey(acmd, value), qd.primaryDatastoreQuery);
    } else {
      String datastorePropName;
      if (ammd.isPrimaryKey()) {
        if (value instanceof Collection) {
          processPotentialBatchGet(qd, (Collection) value, acmd, op);
          List<Key> keys = Utils.newArrayList();
          for (Object obj : ((Collection<?>) value)) {
            keys.add(internalPkToKey(acmd, obj));
          }
          value = keys;
        } else {
          value = internalPkToKey(acmd, value);
        }
        datastorePropName = Entity.KEY_RESERVED_PROPERTY;
      } else {
        datastorePropName = determinePropertyName(ammd);
      }
      value = pojoParamToDatastoreParam(value);
      if (qd.isOrExpression) {
        addLeftPrimaryOrExpression(qd, datastorePropName, value);
      } else {
        if (value instanceof Collection) {
          // DataNuc compiles IN to EQUALS.  If we receive a Collection
          // and the operator is EQUALS we turn it into IN.
          if (op == Query.FilterOperator.EQUAL) {
            op = Query.FilterOperator.IN;
          } else {
            throw new UnsupportedDatastoreFeatureException(
                "Collection parameters are only supported for equality filters.");
          }
        }
        try {
          datastoreQuery.addFilter(datastorePropName, op, value);
        } catch (IllegalArgumentException iae) {
          throw DatastoreExceptionTranslator.wrapIllegalArgumentException(iae);
        }
      }
    }
  }

  private void addLeftPrimaryOrExpression(QueryData qd, String datastorePropName, Object value) {
    List<Object> valueList;
    if (qd.currentOrProperty == null) {
      qd.currentOrProperty = datastorePropName;
    } else if (!qd.currentOrProperty.equals(datastorePropName)) {
      throw new UnsupportedDatastoreFeatureException(
          "Or filters cannot be applied to multiple properties (found both "
          + qd.currentOrProperty + " and "+ datastorePropName + ").");
    }
    valueList = qd.inFilters.get(datastorePropName);
    if (valueList == null) {
      valueList = Utils.newArrayList();
      qd.inFilters.put(datastorePropName, valueList);
    }
    if (value instanceof Iterable) {
      for (Object v : ((Iterable) value)) {
        valueList.add(v);
      }
    } else {
      valueList.add(value);
    }
  }

  private AbstractClassMetaData getJoinClassMetaData(Expression expr, List<String> tuples, QueryData qd) {
    if (expr instanceof VariableExpression) {
      // Change the class meta data to the meta-data for the joined class
      if (qd.joinVariableExpression == null) {
        throw new NucleusFatalUserException(
            query.getSingleStringQuery()
            + ": Encountered a variable expression that isn't part of a join.  Maybe you're "
            + "referencing a non-existent field of an embedded class.");
      }
      if (!((VariableExpression) expr).getId().equals(qd.joinVariableExpression.getId())) {
        throw new NucleusFatalUserException(
            query.getSingleStringQuery()
            + ": Encountered a variable (" + ((VariableExpression) expr).getId()
            + ") that doesn't match the join variable ("
            + qd.joinVariableExpression.getId() + ")");
      }
      Class<?> joinedClass = getSymbolTable().getSymbol(qd.joinVariableExpression.getId()).getValueType();
      return getMetaDataManager().getMetaDataForClass(joinedClass, getClassLoaderResolver());
    }
    Symbol sym = getSymbolTable().getSymbol(tuples.get(0));
    tuples.remove(0);
    return getMetaDataManager().getMetaDataForClass(sym.getValueType(), getClassLoaderResolver());
  }

  private OrderExpression createJoinOrderExpression(PrimaryExpression expression) {
    PrimaryExpression primaryOrderExpr = new PrimaryExpression(expression.getTuples());
    return new OrderExpression(primaryOrderExpr);
  }

  private void processPotentialBatchGet(QueryData qd, Collection value,
                                 AbstractClassMetaData acmd, Query.FilterOperator op) {
    if (!op.equals(Query.FilterOperator.EQUAL)) {
      throw new NucleusFatalUserException(
          "Batch lookup by primary key is only supported with the equality operator.");
    }
    // If it turns out there aren't any other filters or sorts we'll fulfill
    // the query using a batch get
    qd.batchGetKeys = Utils.newLinkedHashSet();
    for (Object obj : value) {
      qd.batchGetKeys.add(internalPkToKey(acmd, obj));
    }
  }

  private Object getValueFromDyadicExpression(Expression expr) {
    // In general we don't support nested dyadic expressions
    // but we special case negation:
    // select * from table where val = -33
    DyadicExpression dyadic = (DyadicExpression) expr;
    if (dyadic.getLeft() instanceof Literal &&
        ((Literal) dyadic.getLeft()).getLiteral() instanceof Number &&
        dyadic.getRight() == null &&
        Expression.OP_NEG.equals(dyadic.getOperator())) {
      Number negateMe = (Number) ((Literal) dyadic.getLeft()).getLiteral();
      return negateNumber(negateMe);
    }

    throw new UnsupportedDatastoreFeatureException(
        "Right side of expression is composed of unsupported components.  "
        + "Left: " + dyadic.getLeft().getClass().getName()
        + ", Op: " + dyadic.getOperator()
        + ", Right: " + dyadic.getRight());
  }

  /**
   * Fetches the tuples of the provided expression, stripping off the first
   * tuple if there are multiple tuples, the table name is aliased, and the
   * first tuple matches the alias.
   */
  private List<String> getTuples(PrimaryExpression expr, String alias) {
    List<String> tuples = Utils.newArrayList();
    tuples.addAll(expr.getTuples());
    return getTuples(tuples, alias);
  }

  static List<String> getTuples(List<String> tuples, String alias) {
    if (alias != null && tuples.size() > 1 && alias.equals(tuples.get(0))) {
      tuples = tuples.subList(1, tuples.size());
    }
    return tuples;
  }

  // TODO(maxr): Use TypeConversionUtils
  private Object pojoParamToDatastoreParam(Object param) {
    if (param instanceof Enum) {
      // TODO Cater for persisting Enum as ordinal. Need the mmd of the other side
      param = ((Enum) param).name();
    } else if (param instanceof byte[]) {
      param = new ShortBlob((byte[]) param);
    } else if (param instanceof Byte[]) {
      param = new ShortBlob(PrimitiveArrays.toByteArray(Arrays.asList((Byte[]) param)));
    } else if (param instanceof BigDecimal) {
      param = ((BigDecimal) param).doubleValue();
    } else if (param instanceof Character) {
      param = param.toString();
    }
    return param;
  }

  private NucleusException noMetaDataException(String member, String fullClassName) {
    return new NucleusFatalUserException(
        "No meta-data for member named " + member + " on class " + fullClassName
            + ".  Are you sure you provided the correct member name in your query?");
  }

  private Object negateNumber(Number negateMe) {
    if (negateMe instanceof BigDecimal) {
      // datastore doesn't support filtering by BigDecimal to convert to double.
      return ((BigDecimal) negateMe).negate().doubleValue();
    } else if (negateMe instanceof Float) {
      return -((Float) negateMe);
    } else if (negateMe instanceof Double) {
      return -((Double) negateMe);
    }
    return -negateMe.longValue();
  }

  JavaTypeMapping getMappingForFieldWithName(List<String> tuples, QueryData qd, AbstractClassMetaData acmd) {
    ClassLoaderResolver clr = getClassLoaderResolver();
    JavaTypeMapping mapping = null;
    // We might be looking for the mapping for a.b.c
    for (String tuple : tuples) {
      DatastoreTable table = qd.tableMap.get(acmd.getFullClassName());
      if (table == null) {
        table = getStoreManager().getDatastoreClass(acmd.getFullClassName(), clr);
        qd.tableMap.put(acmd.getFullClassName(), table);
      }
      // deepest mapping we have so far
      AbstractMemberMetaData mmd = acmd.getMetaDataForMember(tuple);
      mapping = table.getMemberMapping(mmd);
      // set the class meta data to the class of the type of the field of the
      // mapping so that we go one deeper if there are any more tuples
      acmd = getMetaDataManager().getMetaDataForClass(mapping.getMemberMetaData().getType(), clr);
    }
    return mapping;
  }

  private AbstractMemberMetaData getMemberMetaDataForTuples(AbstractClassMetaData acmd, List<String> tuples) {
    AbstractMemberMetaData ammd = acmd.getMetaDataForMember(tuples.get(0));
    if (ammd == null || tuples.size() == 1) {
      return ammd;
    }

    // more than one tuple, so it must be embedded data
    String parentFullClassName = acmd.getFullClassName();
    for (String tuple : tuples.subList(1, tuples.size())) {
      EmbeddedMetaData emd = ammd.getEmbeddedMetaData();
      if (emd == null) {
        throw new NucleusFatalUserException(
            query.getSingleStringQuery() + ": Can only reference properties of a sub-object if "
            + "the sub-object is embedded.");
      }
      DatastoreTable parentTable =
          getStoreManager().getDatastoreClass(parentFullClassName, getClassLoaderResolver());
      parentFullClassName = ammd.getTypeName();
      AbstractMemberMetaData parentField = (AbstractMemberMetaData) emd.getParent();
      EmbeddedMapping embeddedMapping =
          (EmbeddedMapping) parentTable.getMappingForFullFieldName(parentField.getFullFieldName());
      ammd = findMemberMetaDataWithName(tuple, embeddedMapping);
      if (ammd == null) {
        break;
      }
    }
    return ammd;
  }

  private AbstractMemberMetaData findMemberMetaDataWithName(String name, EmbeddedMapping embeddedMapping) {
    int numMappings = embeddedMapping.getNumberOfJavaTypeMappings();
    for (int i = 0; i < numMappings; i++) {
      JavaTypeMapping fieldMapping = embeddedMapping.getJavaTypeMapping(i);
      if (fieldMapping.getMemberMetaData().getName().equals(name)) {
        return fieldMapping.getMemberMetaData();
      }
    }
    // Not ok, but caller knows what to do
    return null;
  }

  private void processPersistableMember(QueryData qd, Query.FilterOperator op, AbstractMemberMetaData ammd, 
      Object value) {
    ClassLoaderResolver clr = getClassLoaderResolver();
    AbstractClassMetaData acmd = getMetaDataManager().getMetaDataForClass(ammd.getType(), clr);
    Object jdoPrimaryKey;
    if (value instanceof Key || value instanceof String) {
      // This is a bit odd, but just to be nice we let users
      // provide the id itself rather than the object containing the id.
      jdoPrimaryKey = value;
    } else if (value instanceof Long || value instanceof Integer) {
      String kind = EntityUtils.determineKind(acmd, getExecutionContext());
      jdoPrimaryKey = KeyFactory.createKey(kind, ((Number) value).longValue());
    } else if (value == null) {
      jdoPrimaryKey = null;
    } else {
      ApiAdapter apiAdapter = getExecutionContext().getApiAdapter();
      jdoPrimaryKey = apiAdapter.getTargetKeyForSingleFieldIdentity(apiAdapter.getIdForObject(value));
      if (jdoPrimaryKey == null) {
        // JDO couldn't find a primary key value on the object, but that doesn't mean
        // the object doesn't have the PK field(s) set, so access it via IdentityUtils
        Object jdoID = apiAdapter.getNewApplicationIdentityObjectId(value, acmd);
        jdoPrimaryKey = apiAdapter.getTargetKeyForSingleFieldIdentity(jdoID);
      }
      if (jdoPrimaryKey == null) {
        throw new NucleusFatalUserException(query.getSingleStringQuery() + ": Parameter value " + value + " does not have an id.");
      }
    }

    Key valueKey = null;
    if (jdoPrimaryKey != null) {
      valueKey = internalPkToKey(acmd, jdoPrimaryKey);
      verifyRelatedKeyIsOfProperType(ammd, valueKey, acmd);
    }

    if (!MetaDataUtils.isOwnedRelation(ammd)) {
      // Add filter on 1-1 relation field
      qd.primaryDatastoreQuery.addFilter(determinePropertyName(ammd), Query.FilterOperator.EQUAL, valueKey);
      return;
    }

    if (!qd.tableMap.get(ammd.getAbstractClassMetaData().getFullClassName()).isParentKeyProvider(ammd)) {
      // Looks like a join.  If it can be satisfied by just extracting the
      // parent key from the provided key, fulfill it.
      if (op != Query.FilterOperator.EQUAL) {
        throw new UnsupportedDatastoreFeatureException(
            "Only the equals operator is supported on conditions involving the owning side of a "
            + "one-to-one.");
      }
      if (valueKey == null) {
        // User is asking for parents where child is null.  Unfortunately we
        // don't have a way to fulfill this because one-to-one is actually
        // implemented as a one-to-many
        throw new NucleusFatalUserException(
            query.getSingleStringQuery() + ": Cannot query for parents with null children.");
      }

      if (valueKey.getParent() == null) {
        throw new NucleusFatalUserException(
            query.getSingleStringQuery() + ": Key of parameter value does not have a parent.");
      }

      // The field is the child side of an owned one to one.  We can just add
      // the parent key to the query as an equality filter on id.
      qd.primaryDatastoreQuery.addFilter(
          Entity.KEY_RESERVED_PROPERTY, Query.FilterOperator.EQUAL, valueKey.getParent());
    } else if (valueKey == null) {
      throw new NucleusFatalUserException(
          query.getSingleStringQuery() + ": The datastore does not support querying for objects with null parents.");
    } else {
      addParentFilter(op, valueKey, qd.primaryDatastoreQuery);
    }
  }

  private void verifyRelatedKeyIsOfProperType(
      AbstractMemberMetaData ammd, Key key, AbstractClassMetaData acmd) {
    String keyKind = key.getKind();
    String fieldKind =
        getIdentifierFactory().newDatastoreContainerIdentifier(acmd).getIdentifierName();
    if (!keyKind.equals(fieldKind)) {
      throw new org.datanucleus.exceptions.NucleusFatalUserException(query.getSingleStringQuery() + ": Field "
                                 + ammd.getFullFieldName() + " maps to kind " + fieldKind + " but"
                                 + " parameter value contains Key of kind " + keyKind );
    }
  }

  private String determinePropertyName(AbstractMemberMetaData ammd) {
    if (ammd.hasExtension(DatastoreManager.PK_ID) ||
        ammd.hasExtension(DatastoreManager.PK_NAME)) {
      // the datsatore doesn't support filtering or sorting by the individual
      // components of the key, so if the field corresponds to one of these
      // components it's a mistake by the user
      throw new org.datanucleus.exceptions.NucleusFatalUserException(query.getSingleStringQuery() + ": Field "
        + ammd.getFullFieldName() + " is a sub-component of the primary key.  The "
        + "datastore does not support filtering or sorting by primary key components, only the "
        + "entire primary key.");
    }
    if (ammd.getColumn() != null) {
      return ammd.getColumn();
    } else if (ammd.getColumnMetaData() != null && ammd.getColumnMetaData().length != 0) {
      return ammd.getColumnMetaData()[0].getName();
    } else if (ammd.getElementMetaData() != null &&
               ammd.getElementMetaData().getColumnMetaData() != null  &&
               ammd.getElementMetaData().getColumnMetaData().length != 0) {
      return ammd.getElementMetaData().getColumnMetaData()[0].getName();
    } else {
      return getIdentifierFactory().newDatastoreFieldIdentifier(ammd.getName()).getIdentifierName();
    }
  }

  private Key internalPkToKey(AbstractClassMetaData acmd, Object internalPk) {
    Key key;
    if (internalPk instanceof String) {
      try {
        key = KeyFactory.stringToKey((String) internalPk);
      } catch (IllegalArgumentException iae) {
        String kind =
            getIdentifierFactory().newDatastoreContainerIdentifier(acmd).getIdentifierName();
        key = KeyFactory.createKey(kind, (String) internalPk);
      }
    } else if (internalPk instanceof Long) {
      String kind =
          getIdentifierFactory().newDatastoreContainerIdentifier(acmd).getIdentifierName();
      key = KeyFactory.createKey(kind, (Long) internalPk);
    } else {
      key = (Key) internalPk;
    }
    return key;
  }

  private void addParentFilter(Query.FilterOperator op, Key key, Query datastoreQuery) {
    // We only support queries on parent if it is an equality filter.
    if (op != Query.FilterOperator.EQUAL) {
      throw new UnsupportedDatastoreFeatureException("Operator is of type " + op + " but the "
          + "datastore only supports parent queries using the equality operator.");
    }

    if (key == null) {
      throw new UnsupportedDatastoreFeatureException(
          "Received a null parent parameter.  The datastore does not support querying for null parents.");
    }
    datastoreQuery.setAncestor(key);
  }

  // Specialization just exists to support tests
  static class UnsupportedDatastoreOperatorException extends NucleusUserException {
    private final String queryString;
    private final Expression.Operator operator;
    private final String msg;

    UnsupportedDatastoreOperatorException(String queryString,
        Expression.Operator operator) {
      this(queryString, operator, null);
    }

    UnsupportedDatastoreOperatorException(String queryString,
        Expression.Operator operator, String msg) {
      super(queryString);
      this.queryString = queryString;
      this.operator = operator;
      this.msg = msg;
    }

    @Override
    public String getMessage() {
      return "Problem with query <" + queryString
          + ">: App Engine datastore does not support operator " + operator + ".  "
          + (msg == null ? "" : msg);
    }

    public Expression.Operator getOperation() {
      return operator;
    }
  }

  private boolean isBulkDelete() {
    return query.getType() == org.datanucleus.store.query.Query.BULK_DELETE;
  }

  // Specialization just exists to support tests
  class UnsupportedDatastoreFeatureException extends NucleusUserException {
    UnsupportedDatastoreFeatureException(String msg) {
      super("Problem with query <" + query.getSingleStringQuery() + ">: " + msg);
    }
  }

  public interface NowProvider {
    Date now();
  }

  public static NowProvider NOW_PROVIDER = new NowProvider() {
    public Date now() {
      return new Date();
    }
  };

  // Keep track of last query for tests
  private transient Query latestDatastoreQuery;

  // Exposed for tests
  Query getLatestDatastoreQuery() {
    return latestDatastoreQuery;
  }

  private ExecutionContext getExecutionContext() {
    return query.getExecutionContext();
  }

  private MetaDataManager getMetaDataManager() {
    return getExecutionContext().getMetaDataManager();
  }

  private ClassLoaderResolver getClassLoaderResolver() {
    return getExecutionContext().getClassLoaderResolver();
  }

  private IdentifierFactory getIdentifierFactory() {
    return getStoreManager().getIdentifierFactory();
  }

  private DatastoreManager getStoreManager() {
    return (DatastoreManager) query.getStoreManager();
  }

  private SymbolTable getSymbolTable() {
    return query.getCompilation().getSymbolTable();
  }
}
