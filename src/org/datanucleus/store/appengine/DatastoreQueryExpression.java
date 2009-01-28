// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import com.google.apphosting.api.datastore.Entity;
import com.google.apphosting.api.datastore.Key;
import com.google.apphosting.api.datastore.KeyFactory;
import com.google.apphosting.api.datastore.Query.SortDirection;
import com.google.apphosting.api.datastore.Query.SortPredicate;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.store.appengine.Utils;
import org.datanucleus.store.mapped.DatastoreContainerObject;
import org.datanucleus.store.mapped.DatastoreField;
import org.datanucleus.store.mapped.DatastoreIdentifier;
import org.datanucleus.store.mapped.MappedStoreManager;
import org.datanucleus.store.mapped.expression.BooleanExpression;
import org.datanucleus.store.mapped.expression.LogicSetExpression;
import org.datanucleus.store.mapped.expression.QueryExpression;
import org.datanucleus.store.mapped.expression.ScalarExpression;
import org.datanucleus.store.mapped.expression.StatementText;
import org.datanucleus.store.mapped.expression.StringLiteral;
import org.datanucleus.store.mapped.mapping.JavaTypeMapping;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

/**
 * Datastore specific implementation of a {@link QueryExpression}.
 * Most functionality is unsupported.  It's currently unclear how much of this
 * actually makes sense for the datastore, since {@link QueryExpression}, even
 * though it doesn't have any rdbms dependencies, still assumes a String-based
 * query mechanism, which the datastore does not have.
 *
 * @author Max Ross <maxr@google.com>
 */
class DatastoreQueryExpression implements QueryExpression {

  private final List<BooleanExpression> andConditions = Utils.newArrayList();
  private final List<SortPredicate> sortPredicates = Utils.newArrayList();
  private final DatastoreTable mainTable;
  private final ClassLoaderResolver clr;
  /**
   * We only set this member if the first sort order is on the pk of the table.
   * If no other sort order is set, we never add it to the list of sort
   * predicates.  If another sort order _is_ set we add this one first.
   */
  private boolean ignoreSubsequentSorts;

  DatastoreQueryExpression(DatastoreTable table, ClassLoaderResolver clr) {
    this.mainTable = table;
    this.clr = clr;
  }

  /**
   * Extract the parent key from the expression.  This is totally fragile and
   * needs to be rewritten.
   *
   * TODO(maxr): Give callers of this method access to the ObjectManager of the
   * owning object (datanuc change).
   *
   * @return The key of the parent in the query.
   */
  Key getParentKey() {
    // We are relying on the query having a single 'and' condition where
    // the first element of the conditoin is a StringLiteral whose value
    // is the string version of the parent key.  Like I said, totally fragile.
    if (andConditions.size() > 1) {
      return null;
    }
    Field appendedField;
    List appended;
    try {
      appendedField = StatementText.class.getDeclaredField("appended");
      appendedField.setAccessible(true);
      StatementText st = andConditions.get(0).toStatementText(ScalarExpression.FILTER);
      appended = (List) appendedField.get(st);
    } catch (NoSuchFieldException e) {
      return null;
    } catch (IllegalAccessException e) {
      return null;
    }
    if (appended.isEmpty()) {
      return null;
    }
    if (!(appended.get(0) instanceof StringLiteral)) {
      return null;
    }
    StringLiteral stringLiteral = (StringLiteral) appended.get(0);
    return KeyFactory.decodeKey(stringLiteral.getValue().toString());
  }

  Collection<SortPredicate> getSortPredicates() {
    return sortPredicates;
  }

  public void setOrdering(ScalarExpression[] exprs, boolean[] descending) {
    if (exprs.length != descending.length) {
      throw new IllegalArgumentException(
          "Expression array and descending array are not the same size.");
    }
    if (ignoreSubsequentSorts) {
      return;
    }
    for (int i = 0; i < exprs.length && !ignoreSubsequentSorts; i++) {
      if (!(exprs[i] instanceof ScalarExpression.DatastoreFieldExpression)) {
        throw new UnsupportedOperationException(
            "Expression of type " + exprs[i].getClass().getName() + " not supported.");
      }
      ScalarExpression.DatastoreFieldExpression dfe = (ScalarExpression.DatastoreFieldExpression) exprs[i];
      String propertyName = dfe.toString();
      boolean isPrimaryKey = isPrimaryKey(propertyName);
      if (isPrimaryKey) {
        // sorting by id requires us to use a reserved property name
        propertyName = Entity.KEY_RESERVED_PROPERTY;
      }
      SortPredicate sortPredicate = new SortPredicate(
          propertyName, descending[i] ? SortDirection.DESCENDING : SortDirection.ASCENDING);
      boolean addPredicate = true;
      if (isPrimaryKey) {
        // User wants to sort by pk.  Since pk is guaranteed to be unique, set a
        // flag so we know there's no point in adding any more sort predicates
        ignoreSubsequentSorts = true;
        // Don't even bother adding if the first sort is id ASC (this is the
        // default sort so there's no point in making the datastore figure this
        // out).
        if (sortPredicate.getDirection() == SortDirection.ASCENDING && sortPredicates.isEmpty()) {
          addPredicate = false;
        }
      }
      if (addPredicate) {
        sortPredicates.add(sortPredicate);
      }
    }
  }

  boolean isPrimaryKey(String propertyName) {
    return mainTable.getDatastoreField(propertyName).isPrimaryKey();
  }

  public void andCondition(BooleanExpression condition) {
    andConditions.add(condition);
  }

  public void andCondition(BooleanExpression condition, boolean unionQueries) {
    // we don't support union so just ignore the unionQueries param
    andConditions.add(condition);
  }

  public void setParent(QueryExpression parentQueryExpr) {
    throw new UnsupportedOperationException();
  }

  public QueryExpression getParent() {
    throw new UnsupportedOperationException();
  }

  public void setCandidateInformation(Class cls, String alias) {
    throw new UnsupportedOperationException();
  }

  public Class getCandidateClass() {
    throw new UnsupportedOperationException();
  }

  public String getCandidateAlias() {
    throw new UnsupportedOperationException();
  }

  public LogicSetExpression getMainTableExpression() {
    return new LogicSetExpression(this, mainTable, null) {

      public String referenceColumn(DatastoreField col) {
        return col.getIdentifier().getIdentifierName();
      }

      public String toString() {
        return null;
      }
    };
  }

  public DatastoreIdentifier getMainTableAlias() {
    throw new UnsupportedOperationException();
  }

  public LogicSetExpression getTableExpression(DatastoreIdentifier alias) {
    return null;
  }

  public LogicSetExpression newTableExpression(DatastoreContainerObject mainTable,
                                               DatastoreIdentifier alias) {
    throw new UnsupportedOperationException();
  }

  public LogicSetExpression[] newTableExpression(DatastoreContainerObject mainTable,
                                                 DatastoreIdentifier alias, boolean unionQueries) {
    throw new UnsupportedOperationException();
  }

  public MappedStoreManager getStoreManager() {
    return mainTable.getStoreManager();
  }

  public ClassLoaderResolver getClassLoaderResolver() {
    return clr;
  }

  public void setDistinctResults(boolean distinctResults) {
    throw new UnsupportedOperationException();
  }

  public void addExtension(String key, Object value) {
    throw new UnsupportedOperationException();
  }

  public Object getValueForExtension(String key) {
    throw new UnsupportedOperationException();
  }

  public HashMap getExtensions() {
    throw new UnsupportedOperationException();
  }

  public boolean hasMetaDataExpression() {
    return false;
  }

  public int[] selectDatastoreIdentity(String alias, boolean unionQueries) {
    throw new UnsupportedOperationException();
  }

  public int[] selectVersion(String alias, boolean unionQueries) {
    throw new UnsupportedOperationException();
  }

  public int[] selectField(String fieldName, String alias, boolean unionQueries) {
    throw new UnsupportedOperationException();
  }

  public int[] select(JavaTypeMapping mapping) {
    throw new UnsupportedOperationException();
  }

  public int[] select(JavaTypeMapping mapping, boolean unionQueries) {
    return null;
  }

  public int selectScalarExpression(ScalarExpression expr) {
    throw new UnsupportedOperationException();
  }

  public int selectScalarExpression(ScalarExpression expr, boolean unionQueries) {
    throw new UnsupportedOperationException();
  }

  public int[] select(DatastoreIdentifier alias, JavaTypeMapping mapping) {
    throw new UnsupportedOperationException();
  }

  public int[] select(DatastoreIdentifier alias, JavaTypeMapping mapping, boolean unionQueries) {
    throw new UnsupportedOperationException();
  }

  public void crossJoin(LogicSetExpression tableExpr, boolean unionQueries) {
    throw new UnsupportedOperationException();
  }

  public boolean hasCrossJoin(LogicSetExpression tableExpr) {
    throw new UnsupportedOperationException();
  }

  public void innerJoin(ScalarExpression expr, ScalarExpression expr2, LogicSetExpression tblExpr,
                        boolean equals, boolean unionQueries) {
    throw new UnsupportedOperationException();
  }

  public void innerJoin(ScalarExpression expr, ScalarExpression expr2, LogicSetExpression tblExpr,
                        boolean equals) {
    throw new UnsupportedOperationException();
  }

  public void leftOuterJoin(ScalarExpression expr, ScalarExpression expr2,
                            LogicSetExpression tblExpr, boolean equals, boolean unionQueries) {
    throw new UnsupportedOperationException();
  }

  public void leftOuterJoin(ScalarExpression expr, ScalarExpression expr2,
                            LogicSetExpression tblExpr, boolean equals) {
    throw new UnsupportedOperationException();
  }

  public void rightOuterJoin(ScalarExpression expr, ScalarExpression expr2,
                             LogicSetExpression tblExpr, boolean equals, boolean unionQueries) {
    throw new UnsupportedOperationException();
  }

  public void rightOuterJoin(ScalarExpression expr, ScalarExpression expr2,
                             LogicSetExpression tblExpr, boolean equals) {
    throw new UnsupportedOperationException();
  }

  public void addGroupingExpression(ScalarExpression expr) {
    throw new UnsupportedOperationException();
  }

  public void setHaving(BooleanExpression expr) {
    throw new UnsupportedOperationException();
  }

  public void setUpdates(ScalarExpression[] exprs) {
    throw new UnsupportedOperationException();
  }

  public void union(QueryExpression qe) {
    throw new UnsupportedOperationException();
  }

  public void iorCondition(BooleanExpression condition) {
    throw new UnsupportedOperationException();
  }

  public void iorCondition(BooleanExpression condition, boolean unionQueries) {
    throw new UnsupportedOperationException();
  }

  public void setRangeConstraint(long offset, long count) {
    throw new UnsupportedOperationException();
  }

  public void setExistsSubQuery(boolean isExistsSubQuery) {
    throw new UnsupportedOperationException();
  }

  public int getNumberOfScalarExpressions() {
    throw new UnsupportedOperationException();
  }

  public StatementText toDeleteStatementText() {
    throw new UnsupportedOperationException();
  }

  public StatementText toUpdateStatementText() {
    throw new UnsupportedOperationException();
  }

  public StatementText toStatementText(boolean lock) {
    throw new UnsupportedOperationException();
  }

  public void reset() {
    throw new UnsupportedOperationException();
  }
}
