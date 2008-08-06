// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine.query;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ManagedConnection;
import org.datanucleus.ObjectManager;
import org.datanucleus.StateManager;
import org.datanucleus.FetchPlan;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.query.expression.Literal;
import org.datanucleus.query.expression.Expression;
import org.datanucleus.query.expression.DyadicExpression;
import org.datanucleus.query.expression.PrimaryExpression;
import org.datanucleus.query.expression.OrderExpression;
import org.datanucleus.store.query.AbstractJDOQLQuery;
import org.datanucleus.store.FieldValues;
import org.datanucleus.store.appengine.DatastoreFieldManager;
import org.datanucleus.util.NucleusLogger;
import com.google.apphosting.api.datastore.DatastoreService;
import com.google.apphosting.api.datastore.Query;
import com.google.apphosting.api.datastore.Entity;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.collect.ImmutableMapBuilder;
import com.google.common.base.Function;


/**
 * Implementation of JDOQL for the app engine datastore.
 *
 * Datanucleus supports in-memory evaluation of queries, but
 * for now we have it disabled and are only allowing queries
 * that can be natively fulfilled by the app engine datastore.
 *
 * TODO(maxr): More logging
 * TODO(maxr): Localized logging
 * TODO(maxr): Localized exception messages.
 *
 * @author Max Ross <maxr@google.com>
 */
public class JDOQLQuery extends AbstractJDOQLQuery {

  // Exposed for testing
  static final Expression.Operator GROUP_BY_OP =
      new Expression.Operator("GROUP BY", Integer.MAX_VALUE);

  // Exposed for testing
  static final Expression.Operator HAVING_OP = 
      new Expression.Operator("HAVING", Integer.MAX_VALUE);

  static final Set<Expression.Operator> UNSUPPORTED_OPERATORS =
      Sets.newHashSet(
          (Expression.Operator)Expression.OP_ADD,
          (Expression.Operator)Expression.OP_BETWEEN,
          (Expression.Operator)Expression.OP_COM,
          (Expression.Operator)Expression.OP_CONCAT,
          (Expression.Operator)Expression.OP_DIV,
          (Expression.Operator)Expression.OP_IS,
          (Expression.Operator)Expression.OP_ISNOT,
          (Expression.Operator)Expression.OP_LIKE,
          (Expression.Operator)Expression.OP_MOD,
          (Expression.Operator)Expression.OP_NEG,
          (Expression.Operator)Expression.OP_MUL,
          (Expression.Operator)Expression.OP_NOT,
          (Expression.Operator)Expression.OP_OR,
          (Expression.Operator)Expression.OP_SUB
      );

  private static final Map<Expression.Operator, Query.FilterOperator> JDO_OP_TO_APPENGINE_OP =
      new ImmutableMapBuilder<Expression.Operator, Query.FilterOperator>()
          .put(Expression.OP_EQ, Query.FilterOperator.EQUAL)
          .put(Expression.OP_GT, Query.FilterOperator.GREATER_THAN)
          .put(Expression.OP_GTEQ, Query.FilterOperator.GREATER_THAN_OR_EQUAL)
          .put(Expression.OP_LT, Query.FilterOperator.LESS_THAN)
          .put(Expression.OP_LTEQ, Query.FilterOperator.LESS_THAN_OR_EQUAL)
          .getMap();

  /**
   * Filters that have been added to the {@link Query}.  Currently only made
   * available to tests.
   */
  private final List<AddedFilter> addedFilters = Lists.newArrayList();

  /**
   * Sorts that have been added to the {@link Query}.  Currently only made
   * available to tests.
   */
  private final List<AddedSort> addedSorts = Lists.newArrayList();

  /**
   * Constructs a new query instance that uses the given object manager.
   *
   * @param om The associated ObjectManager for this query.
   */
  public JDOQLQuery(ObjectManager om) {
    this(om, (JDOQLQuery) null);
  }

  /**
   * Constructs a new query instance having the same criteria as the given query.
   *
   * @param om The ObjectManager.
   * @param q The query from which to copy criteria.
   */
  public JDOQLQuery(ObjectManager om, JDOQLQuery q) {
    super(om, q);
  }

  /**
   * Constructor for a JDOQL query where the query is specified using the "Single-String" format.
   *
   * @param om The object manager.
   * @param query The JDOQL query string.
   */
  public JDOQLQuery(ObjectManager om, String query) {
    super(om, query);
  }

  /**
   * {@inheritDoc}
   *
   * We'd like to return {@link Iterable} instead but
   * {@link javax.persistence.Query#getResultList()} returns {@link List}.
   */
  @Override
  protected List<?> performExecute(Map parameters) {
    validate();

    long startTime = System.currentTimeMillis();
    if (NucleusLogger.QUERY.isDebugEnabled()) {
      NucleusLogger.QUERY.debug(LOCALISER.msg("021046", "JDOQL", getSingleStringQuery(), null));
    }
    ManagedConnection mconn = om.getStoreManager().getConnection(om);
    try {
      DatastoreService ds = (DatastoreService) mconn.getConnection();
      // TODO(maxr): Don't force users to use fqn as the kind.
      Query q = new Query(candidateClassName);
      addFilters(q, parameters);
      addSorts(q);
      Iterable<Entity> entities = ds.prepare(q).asIterable();
      if (NucleusLogger.QUERY.isDebugEnabled()) {
        NucleusLogger.QUERY.debug(LOCALISER.msg("021074", "JDOQL",
            "" + (System.currentTimeMillis() - startTime)));
      }
      final ClassLoaderResolver clr = om.getClassLoaderResolver();
      final AbstractClassMetaData acmd =
          om.getMetaDataManager().getMetaDataForClass(candidateClass, clr);

      Function<Entity, Object> entityToPojoFunc = new Function<Entity, Object>() {
        public Object apply(Entity entity) {
          return entityToPojo(entity, acmd, clr);
        }
      };
      return new StreamingQueryResult(this, entities, entityToPojoFunc);
    } finally {
      mconn.release();
    }
  }

  private Object entityToPojo(final Entity entity, final AbstractClassMetaData acmd,
      final ClassLoaderResolver clr) {
    FieldValues fv = new FieldValues() {
      public void fetchFields(StateManager sm) {
        sm.replaceFields(acmd.getAllMemberPositions(), new DatastoreFieldManager(sm, entity));
      }
      public void fetchNonLoadedFields(StateManager sm) {
        sm.replaceNonLoadedFields(
            acmd.getAllMemberPositions(), new DatastoreFieldManager(sm, entity));
      }
      public FetchPlan getFetchPlanForLoading() {
        return null;
      }
    };
    return om.findObjectUsingAID(clr.classForName(acmd.getFullClassName()), fv, ignoreCache, true);

  }

  private void validate() {
    // We don't support in-memory query fulfillment, so if the query contains
    // a grouping or a having it's automatically an error.
    if (grouping != null) {
      throw new UnsupportedJDOQLOperatorException(getSingleStringQuery(), GROUP_BY_OP);
    }

    if (having != null) {
      throw new UnsupportedJDOQLOperatorException(getSingleStringQuery(), HAVING_OP);
    }
  }

  /**
   * Adds sorts to the given {@link Query} by examining the compiled order
   * expression.
   */
  private void addSorts(Query q) {
    // Just parse by hand for now
    Expression[] orderBys = compilation.getExprOrdering();
    if (orderBys == null) {
      return;
    }
    for (Expression expr : orderBys) {
      OrderExpression oe = (OrderExpression) expr;
      Query.SortDirection dir = 
          oe.getSortOrder().equals("ascending") ? Query.SortDirection.ASCENDING : Query.SortDirection.DESCENDING;
      String sortProp = ((PrimaryExpression)oe.getLeft()).getId();
      q.addSort(sortProp, dir);
      addedSorts.add(new AddedSort(sortProp, dir));
    }
  }

  /**
   * Adds filters to the given {@link Query} by examining the compiled filter
   * expression.
   */
  private void addFilters(Query q, Map parameters) {
    // Just parse by hand for now
    Expression filter = compilation.getExprFilter();
    addExpression(filter, q, parameters);
  }

  /**
   * Recursively walks the given expression, adding filters to the given
   * {@link Query} where appropriate.
   *
   * @throws UnsupportedJDOQLOperatorException If we encounter a JDOQL operator
   * that we don't support.
   * @throws UnsupportedJDOQLFeatureException If the query uses a JDOQL feature
   * that we don't support.
   *
   * @TODO(maxr): Get rid of the path param once we have more confidence in
   * this code.
   */
  private void addExpression(Expression expr, Query q, Map parameters) {
    if (expr == null) {
      return;
    }
    checkForUnsupportedOperator(expr.getOperator());
    if (expr instanceof DyadicExpression) {
      if (expr.getOperator().equals(Expression.OP_AND)) {
        addExpression(expr.getLeft(), q, parameters);
        addExpression(expr.getRight(), q, parameters);
      } else if(JDO_OP_TO_APPENGINE_OP.get(expr.getOperator()) == null) {
        throw new UnsupportedJDOQLOperatorException(getSingleStringQuery(), expr.getOperator());
      } else if (expr.getLeft() instanceof PrimaryExpression) {
        addLeftPrimaryExpression(
            (PrimaryExpression) expr.getLeft(),
            expr.getOperator(),
            expr.getRight(),
            q,
            parameters);
      } else {
        // Recurse!
        addExpression(expr.getLeft(), q, parameters);
        addExpression(expr.getRight(), q, parameters);
      }
    } else if (expr instanceof PrimaryExpression) {
      // Recurse!
      addExpression(expr.getLeft(), q, parameters);
      addExpression(expr.getRight(), q, parameters);
    } else {
      throw new UnsupportedJDOQLFeatureException("Unexpected expression type while parsing "
          + getSingleStringQuery() + ": " + expr.getClass().getName());
    }
  }

  private void addLeftPrimaryExpression(PrimaryExpression left, Expression.Operator operator,
      Expression right, Query q, Map parameters) {
    String propName = left.getId();
    Query.FilterOperator op = JDO_OP_TO_APPENGINE_OP.get(operator);
    Object value;
    if (right instanceof PrimaryExpression) {
      value = parameters.get(((PrimaryExpression)right).getId());
    } else if (right instanceof Literal) {
      value = ((Literal)right).getLiteral();
    } else {
      // We hit an operator that is not in the unsupported list and does
      // not have an entry in JDO_OP_TO_APPENGINE_OP.  Almost certainly
      // a programming error.
      throw new UnsupportedJDOQLFeatureException(getSingleStringQuery());
    }
    q.addFilter(propName, op, value);
    addedFilters.add(new AddedFilter(propName, op, value));
  }

  private void checkForUnsupportedOperator(Expression.Operator operator) {
    if (UNSUPPORTED_OPERATORS.contains(operator)) {
      throw new UnsupportedJDOQLOperatorException(getSingleStringQuery(), operator);
    }
  }

  // Exposed for tests
  // TODO(maxr): Remove once filters can be retrieved from the Query object
  List<AddedFilter> getAddedFilters() {
    return addedFilters;
  }

  // Exposed for tests
  // TODO(maxr): Remove once sorts can be retrieved from the Query object
  List<AddedSort> getAddedSorts() {
    return addedSorts;
  }


  // Specialization just exists to support tests
  static class UnsupportedJDOQLOperatorException extends UnsupportedOperationException {
    private final String queryString;
    private final Expression.Operator operator;

    UnsupportedJDOQLOperatorException(String queryString, Expression.Operator operator) {
      super(queryString);
      this.queryString = queryString;
      this.operator = operator;
    }

    public String getMessage() {
      return "Problem with query <" + queryString
          + ">: App Engine datastore does not support operator " + operator;
    }

    public Expression.Operator getOperation() {
      return operator;
    }
  }

  // Specialization just exists to support tests
  static class UnsupportedJDOQLFeatureException extends UnsupportedOperationException {
    private final String queryString;

    UnsupportedJDOQLFeatureException(String queryString) {
      super(queryString);
      this.queryString = queryString;
    }

    public String getMessage() {
      return "Problem with query <" + queryString
          + ">: App Engine datastore does not support one or more features of this query.";
    }
  }
}