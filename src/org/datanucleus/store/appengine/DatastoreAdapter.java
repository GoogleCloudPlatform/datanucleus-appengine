// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ManagedConnection;
import org.datanucleus.store.mapped.DatastoreContainerObject;
import org.datanucleus.store.mapped.DatastoreIdentifier;
import org.datanucleus.store.mapped.IdentifierType;
import org.datanucleus.store.mapped.MappedStoreManager;
import org.datanucleus.store.mapped.expression.BooleanExpression;
import org.datanucleus.store.mapped.expression.CharacterExpression;
import org.datanucleus.store.mapped.expression.LogicSetExpression;
import org.datanucleus.store.mapped.expression.NumericExpression;
import org.datanucleus.store.mapped.expression.QueryExpression;
import org.datanucleus.store.mapped.expression.ScalarExpression;
import org.datanucleus.store.mapped.expression.StringExpression;
import org.datanucleus.store.mapped.expression.StringLiteral;
import org.datanucleus.store.mapped.mapping.MappingManager;
import org.datanucleus.store.schema.StoreSchemaHandler;

import java.sql.Timestamp;
import java.util.Collection;
import java.util.HashSet;

/**
 * Adapter for the App Engine datastore.
 * TODO(maxr): Don't extend rdbms.adapter.DatabaseAdapter
 * Currently necessary to get the identifier factory stuff
 * to work.
 *
 * @author Max Ross <maxr@google.com>
 */
class DatastoreAdapter implements org.datanucleus.store.mapped.DatastoreAdapter {

  public DatastoreAdapter() {
    supportedOptions.add(IDENTITY_COLUMNS);
    supportedOptions.add(IDENTIFIERS_MIXEDCASE);
    supportedOptions.add(IDENTIFIERS_LOWERCASE);
    supportedOptions.add(IDENTIFIERS_UPPERCASE);
  }

  protected Collection<String> supportedOptions = new HashSet<String>();

  public MappingManager getMappingManager(MappedStoreManager mappedStoreManager) {
    return new DatastoreMappingManager(mappedStoreManager);
  }

  public void initialiseDatastore(Object conn) {
  }

  public void initialiseTypes(StoreSchemaHandler handler, ManagedConnection mconn) {
  }

  public void removeUnsupportedMappings(StoreSchemaHandler handler, ManagedConnection mconn) {
  }

  public Collection<String> getSupportedOptions() {
    return supportedOptions;
  }

  public boolean supportsOption(String option) {
    return supportedOptions.contains(option);
  }

  public long getAdapterTime(Timestamp time) {
    return -1;
  }

  public int getDatastoreMajorVersion() {
    return 0;
  }

  public int getDatastoreMinorVersion() {
    return 0;
  }

  public boolean supportsQueryFetchSize(int size) {
    return true;
  }

  public String getVendorID() {
    return "Google";
  }

  public boolean isReservedKeyword(String word) {
    return false;
  }

  public String getIdentifierQuoteString() {
    return "\"";
  }

  public int getDatastoreIdentifierMaxLength(IdentifierType identifierType) {
    return 99;
  }

  public int getMaxForeignKeys() {
    return 999;
  }

  public int getMaxIndexes() {
    return 999;
  }

  public String getCatalogSeparator() {
    return null;
  }

  public boolean isIdentityFieldDataType(String typeName) {
    return false;
  }

  public String toString() {
    return "Google App Engine Datastore";
  }

  public ScalarExpression getCurrentDateMethod(QueryExpression qs) {
    return null;
  }

  public ScalarExpression getCurrentTimeMethod(QueryExpression qs) {
    return null;
  }

  public ScalarExpression getCurrentTimestampMethod(QueryExpression qs) {
    return null;
  }

  public NumericExpression modOperator(ScalarExpression operand1, ScalarExpression operand2) {
    return null;
  }

  public ScalarExpression getEscapedPatternExpression(ScalarExpression patternExpression) {
    return null;
  }

  public String getPatternExpressionAnyCharacter() {
    return null;
  }

  public String getPatternExpressionZeroMoreCharacters() {
    return null;
  }

  public String getEscapePatternExpression() {
    return null;
  }

  public String getEscapeCharacter() {
    return null;
  }

  public String cartersianProduct(LogicSetExpression Y) {
    return null;
  }

  public QueryExpression newQueryStatement(DatastoreContainerObject table, DatastoreIdentifier rangeVar, ClassLoaderResolver clr) {
    return null;
  }

  public NumericExpression toNumericExpression(CharacterExpression expr) {
    return null;
  }

  public StringExpression translateMethod(ScalarExpression expr, ScalarExpression toExpr, ScalarExpression fromExpr) {
    return null;
  }

  public NumericExpression getNumericExpressionForMethod(String method, ScalarExpression expr) {
    return null;
  }

  public BooleanExpression endsWithMethod(ScalarExpression leftOperand, ScalarExpression rightOperand) {
    return null;
  }

  public BooleanExpression matchesMethod(StringExpression text, StringExpression pattern) {
    return null;
  }

  public StringExpression toStringExpression(NumericExpression expr) {
    return null;
  }

  public StringExpression toStringExpression(StringLiteral expr) {
    return null;
  }

  public StringExpression substringMethod(StringExpression str,
                                          NumericExpression begin) {
    return null;
  }

  public StringExpression lowerMethod(StringExpression str) {
    return null;
  }

  public StringExpression upperMethod(StringExpression str) {
    return null;
  }

  public StringExpression trimMethod(StringExpression str, boolean leading, boolean trailing) {
    return null;
  }

  public StringExpression substringMethod(StringExpression str,
                                          NumericExpression begin,
                                          NumericExpression end) {
    return null;
  }

  public BooleanExpression startsWithMethod(ScalarExpression source, ScalarExpression str) {
    return null;
  }

  public NumericExpression indexOfMethod(ScalarExpression source, ScalarExpression str, NumericExpression from) {
    return null;
  }

  public String getOperatorConcat() {
    return null;
  }

  public ScalarExpression concatOperator(ScalarExpression operand1, ScalarExpression operand2) {
    return null;
  }
}
