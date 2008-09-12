// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.store.appengine;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.Connection;
import java.sql.RowIdLifetime;

/**
 * {@link DatabaseMetaData} implementation for the App Engine datastore.
 * Most of this simply isn't relevant to the datastore.
 *
 * TODO(maxr): Come up with reasonable values for the methods that are relevant
 * and add comments to the methods that aren't relevant explaining why.
 *
 * @author Max Ross <maxr@google.com>
 */
class DatastoreMetaData implements DatabaseMetaData {

  public String getDatabaseProductName() throws SQLException {
    return "Google App Engine Datastore";
  }

  public String getDatabaseProductVersion() throws SQLException {
    return "1.0";
  }

  public String getSQLKeywords() throws SQLException {
    return "";
  }

  public int getMaxColumnNameLength() throws SQLException {
    // TODO(maxr): figure out the real limit
    // http://www.jpox.org/servlet/jira/browse/NUCRDBMS-57
    return 100;
  }

  public int getMaxTableNameLength() throws SQLException {
    // TODO(maxr): figure out the real limit
    // http://www.jpox.org/servlet/jira/browse/NUCRDBMS-57
    return 100;
  }

  public boolean allProceduresAreCallable() throws SQLException {
    return false;
  }

  public boolean allTablesAreSelectable() throws SQLException {
    return false;
  }

  public String getURL() throws SQLException {
    return null;
  }

  public String getUserName() throws SQLException {
    return null;
  }

  public boolean isReadOnly() throws SQLException {
    return false;
  }

  public boolean nullsAreSortedHigh() throws SQLException {
    return false;
  }

  public boolean nullsAreSortedLow() throws SQLException {
    return false;
  }

  public boolean nullsAreSortedAtStart() throws SQLException {
    return false;
  }

  public boolean nullsAreSortedAtEnd() throws SQLException {
    return false;
  }

  public String getDriverName() throws SQLException {
    return null;
  }

  public String getDriverVersion() throws SQLException {
    return null;
  }

  public int getDriverMajorVersion() {
    return 0;
  }

  public int getDriverMinorVersion() {
    return 0;
  }

  public boolean usesLocalFiles() throws SQLException {
    return false;
  }

  public boolean usesLocalFilePerTable() throws SQLException {
    return false;
  }

  public boolean supportsMixedCaseIdentifiers() throws SQLException {
    return true;
  }

  public boolean storesUpperCaseIdentifiers() throws SQLException {
    return false;
  }

  public boolean storesLowerCaseIdentifiers() throws SQLException {
    return false;
  }

  public boolean storesMixedCaseIdentifiers() throws SQLException {
    return false;
  }

  public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
    return true;
  }

  public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
    return false;
  }

  public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
    return false;
  }

  public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
    return false;
  }

  public String getIdentifierQuoteString() throws SQLException {
    return null;
  }

  public String getNumericFunctions() throws SQLException {
    return null;
  }

  public String getStringFunctions() throws SQLException {
    return null;
  }

  public String getSystemFunctions() throws SQLException {
    return null;
  }

  public String getTimeDateFunctions() throws SQLException {
    return null;
  }

  public String getSearchStringEscape() throws SQLException {
    return null;
  }

  public String getExtraNameCharacters() throws SQLException {
    return null;
  }

  public boolean supportsAlterTableWithAddColumn() throws SQLException {
    return false;
  }

  public boolean supportsAlterTableWithDropColumn() throws SQLException {
    return false;
  }

  public boolean supportsColumnAliasing() throws SQLException {
    return false;
  }

  public boolean nullPlusNonNullIsNull() throws SQLException {
    return false;
  }

  public boolean supportsConvert() throws SQLException {
    return false;
  }

  public boolean supportsConvert(int i, int i1) throws SQLException {
    return false;
  }

  public boolean supportsTableCorrelationNames() throws SQLException {
    return false;
  }

  public boolean supportsDifferentTableCorrelationNames() throws SQLException {
    return false;
  }

  public boolean supportsExpressionsInOrderBy() throws SQLException {
    return false;
  }

  public boolean supportsOrderByUnrelated() throws SQLException {
    return false;
  }

  public boolean supportsGroupBy() throws SQLException {
    return false;
  }

  public boolean supportsGroupByUnrelated() throws SQLException {
    return false;
  }

  public boolean supportsGroupByBeyondSelect() throws SQLException {
    return false;
  }

  public boolean supportsLikeEscapeClause() throws SQLException {
    return false;
  }

  public boolean supportsMultipleResultSets() throws SQLException {
    return false;
  }

  public boolean supportsMultipleTransactions() throws SQLException {
    return false;
  }

  public boolean supportsNonNullableColumns() throws SQLException {
    return false;
  }

  public boolean supportsMinimumSQLGrammar() throws SQLException {
    return false;
  }

  public boolean supportsCoreSQLGrammar() throws SQLException {
    return false;
  }

  public boolean supportsExtendedSQLGrammar() throws SQLException {
    return false;
  }

  public boolean supportsANSI92EntryLevelSQL() throws SQLException {
    return false;
  }

  public boolean supportsANSI92IntermediateSQL() throws SQLException {
    return false;
  }

  public boolean supportsANSI92FullSQL() throws SQLException {
    return false;
  }

  public boolean supportsIntegrityEnhancementFacility() throws SQLException {
    return false;
  }

  public boolean supportsOuterJoins() throws SQLException {
    return false;
  }

  public boolean supportsFullOuterJoins() throws SQLException {
    return false;
  }

  public boolean supportsLimitedOuterJoins() throws SQLException {
    return false;
  }

  public String getSchemaTerm() throws SQLException {
    return null;
  }

  public String getProcedureTerm() throws SQLException {
    return null;
  }

  public String getCatalogTerm() throws SQLException {
    return null;
  }

  public boolean isCatalogAtStart() throws SQLException {
    return false;
  }

  public String getCatalogSeparator() throws SQLException {
    return null;
  }

  public boolean supportsSchemasInDataManipulation() throws SQLException {
    return false;
  }

  public boolean supportsSchemasInProcedureCalls() throws SQLException {
    return false;
  }

  public boolean supportsSchemasInTableDefinitions() throws SQLException {
    return false;
  }

  public boolean supportsSchemasInIndexDefinitions() throws SQLException {
    return false;
  }

  public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
    return false;
  }

  public boolean supportsCatalogsInDataManipulation() throws SQLException {
    return false;
  }

  public boolean supportsCatalogsInProcedureCalls() throws SQLException {
    return false;
  }

  public boolean supportsCatalogsInTableDefinitions() throws SQLException {
    return false;
  }

  public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
    return false;
  }

  public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
    return false;
  }

  public boolean supportsPositionedDelete() throws SQLException {
    return false;
  }

  public boolean supportsPositionedUpdate() throws SQLException {
    return false;
  }

  public boolean supportsSelectForUpdate() throws SQLException {
    return false;
  }

  public boolean supportsStoredProcedures() throws SQLException {
    return false;
  }

  public boolean supportsSubqueriesInComparisons() throws SQLException {
    return false;
  }

  public boolean supportsSubqueriesInExists() throws SQLException {
    return false;
  }

  public boolean supportsSubqueriesInIns() throws SQLException {
    return false;
  }

  public boolean supportsSubqueriesInQuantifieds() throws SQLException {
    return false;
  }

  public boolean supportsCorrelatedSubqueries() throws SQLException {
    return false;
  }

  public boolean supportsUnion() throws SQLException {
    return false;
  }

  public boolean supportsUnionAll() throws SQLException {
    return false;
  }

  public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
    return false;
  }

  public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
    return false;
  }

  public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
    return false;
  }

  public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
    return false;
  }

  public int getMaxBinaryLiteralLength() throws SQLException {
    return 0;
  }

  public int getMaxCharLiteralLength() throws SQLException {
    return 0;
  }

  public int getMaxColumnsInGroupBy() throws SQLException {
    return 0;
  }

  public int getMaxColumnsInIndex() throws SQLException {
    return 0;
  }

  public int getMaxColumnsInOrderBy() throws SQLException {
    return 0;
  }

  public int getMaxColumnsInSelect() throws SQLException {
    return 0;
  }

  public int getMaxColumnsInTable() throws SQLException {
    return 0;
  }

  public int getMaxConnections() throws SQLException {
    return 0;
  }

  public int getMaxCursorNameLength() throws SQLException {
    return 0;
  }

  public int getMaxIndexLength() throws SQLException {
    return 0;
  }

  public int getMaxSchemaNameLength() throws SQLException {
    return 0;
  }

  public int getMaxProcedureNameLength() throws SQLException {
    return 0;
  }

  public int getMaxCatalogNameLength() throws SQLException {
    return 0;
  }

  public int getMaxRowSize() throws SQLException {
    return 0;
  }

  public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
    return false;
  }

  public int getMaxStatementLength() throws SQLException {
    return 0;
  }

  public int getMaxStatements() throws SQLException {
    return 0;
  }

  public int getMaxTablesInSelect() throws SQLException {
    return 0;
  }

  public int getMaxUserNameLength() throws SQLException {
    return 0;
  }

  public int getDefaultTransactionIsolation() throws SQLException {
    return 0;
  }

  public boolean supportsTransactions() throws SQLException {
    return false;
  }

  public boolean supportsTransactionIsolationLevel(int i) throws SQLException {
    return false;
  }

  public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
    return false;
  }

  public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
    return false;
  }

  public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
    return false;
  }

  public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
    return false;
  }

  public ResultSet getProcedures(String s, String s1, String s2) throws SQLException {
    return null;
  }

  public ResultSet getProcedureColumns(String s, String s1, String s2, String s3)
      throws SQLException {
    return null;
  }

  public ResultSet getTables(String s, String s1, String s2, String[] strings) throws SQLException {
    return null;
  }

  public ResultSet getSchemas() throws SQLException {
    return null;
  }

  public ResultSet getCatalogs() throws SQLException {
    return null;
  }

  public ResultSet getTableTypes() throws SQLException {
    return null;
  }

  public ResultSet getColumns(String s, String s1, String s2, String s3) throws SQLException {
    return null;
  }

  public ResultSet getColumnPrivileges(String s, String s1, String s2, String s3)
      throws SQLException {
    return null;
  }

  public ResultSet getTablePrivileges(String s, String s1, String s2) throws SQLException {
    return null;
  }

  public ResultSet getBestRowIdentifier(String s, String s1, String s2, int i, boolean b)
      throws SQLException {
    return null;
  }

  public ResultSet getVersionColumns(String s, String s1, String s2) throws SQLException {
    return null;
  }

  public ResultSet getPrimaryKeys(String s, String s1, String s2) throws SQLException {
    return null;
  }

  public ResultSet getImportedKeys(String s, String s1, String s2) throws SQLException {
    return null;
  }

  public ResultSet getExportedKeys(String s, String s1, String s2) throws SQLException {
    return null;
  }

  public ResultSet getCrossReference(String s, String s1, String s2, String s3, String s4,
      String s5) throws SQLException {
    return null;
  }

  public ResultSet getTypeInfo() throws SQLException {
    return null;
  }

  public ResultSet getIndexInfo(String s, String s1, String s2, boolean b, boolean b1)
      throws SQLException {
    return null;
  }

  public boolean supportsResultSetType(int i) throws SQLException {
    return false;
  }

  public boolean supportsResultSetConcurrency(int i, int i1) throws SQLException {
    return false;
  }

  public boolean ownUpdatesAreVisible(int i) throws SQLException {
    return false;
  }

  public boolean ownDeletesAreVisible(int i) throws SQLException {
    return false;
  }

  public boolean ownInsertsAreVisible(int i) throws SQLException {
    return false;
  }

  public boolean othersUpdatesAreVisible(int i) throws SQLException {
    return false;
  }

  public boolean othersDeletesAreVisible(int i) throws SQLException {
    return false;
  }

  public boolean othersInsertsAreVisible(int i) throws SQLException {
    return false;
  }

  public boolean updatesAreDetected(int i) throws SQLException {
    return false;
  }

  public boolean deletesAreDetected(int i) throws SQLException {
    return false;
  }

  public boolean insertsAreDetected(int i) throws SQLException {
    return false;
  }

  public boolean supportsBatchUpdates() throws SQLException {
    return false;
  }

  public ResultSet getUDTs(String s, String s1, String s2, int[] ints) throws SQLException {
    return null;
  }

  public Connection getConnection() throws SQLException {
    return null;
  }

  public boolean supportsSavepoints() throws SQLException {
    return false;
  }

  public boolean supportsNamedParameters() throws SQLException {
    return false;
  }

  public boolean supportsMultipleOpenResults() throws SQLException {
    return false;
  }

  public boolean supportsGetGeneratedKeys() throws SQLException {
    return false;
  }

  public ResultSet getSuperTypes(String s, String s1, String s2) throws SQLException {
    return null;
  }

  public ResultSet getSuperTables(String s, String s1, String s2) throws SQLException {
    return null;
  }

  public ResultSet getAttributes(String s, String s1, String s2, String s3) throws SQLException {
    return null;
  }

  public boolean supportsResultSetHoldability(int i) throws SQLException {
    return false;
  }

  public int getResultSetHoldability() throws SQLException {
    return 0;
  }

  public int getDatabaseMajorVersion() throws SQLException {
    return 0;
  }

  public int getDatabaseMinorVersion() throws SQLException {
    return 0;
  }

  public int getJDBCMajorVersion() throws SQLException {
    return 0;
  }

  public int getJDBCMinorVersion() throws SQLException {
    return 0;
  }

  public int getSQLStateType() throws SQLException {
    return 0;
  }

  public boolean locatorsUpdateCopy() throws SQLException {
    return false;
  }

  public boolean supportsStatementPooling() throws SQLException {
    return false;
  }

  public RowIdLifetime getRowIdLifetime() throws SQLException {
    return null;
  }

  public ResultSet getSchemas(String s, String s1) throws SQLException {
    return null;
  }

  public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
    return false;
  }

  public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
    return false;
  }

  public ResultSet getClientInfoProperties() throws SQLException {
    return null;
  }

  public ResultSet getFunctions(String s, String s1, String s2) throws SQLException {
    return null;
  }

  public ResultSet getFunctionColumns(String s, String s1, String s2, String s3)
      throws SQLException {
    return null;
  }

  public <T> T unwrap(Class<T> tClass) throws SQLException {
    return null;
  }

  public boolean isWrapperFor(Class<?> aClass) throws SQLException {
    return false;
  }
}
