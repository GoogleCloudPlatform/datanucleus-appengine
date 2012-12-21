/**********************************************************************
Copyright (c) 2005 Erik Bengtson and others. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 

Contributors:
    ...
**********************************************************************/
package org.datanucleus.store.mapped;

import java.sql.Timestamp;
import java.util.Collection;
import java.util.Map;

import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.mapped.mapping.MappingManager;
import org.datanucleus.store.schema.StoreSchemaHandler;

/**
 * Definition of a datastore adapter.
 */
public interface DatastoreAdapter
{
    /**
     * Whether this datastore adapter support identity fields.
     * In SQL this would be things like "AUTOINCREMENT", "IDENTITY", "SERIAL".
     */
    public static final String IDENTITY_COLUMNS = "IdentityColumns";

    /** Whether we support sequences. */
    public static final String SEQUENCES = "Sequences";

    /** Whether "Types.BIT" is really mapped as BOOLEAN. */
    public static final String BIT_IS_REALLY_BOOLEAN = "BitIsReallyBoolean";

    /** Whether we support Boolean comparisons. */
    public static final String BOOLEAN_COMPARISON = "BooleanExpression";

    public static final String ESCAPE_EXPRESSION_IN_LIKE_PREDICATE = "EscapeExpressionInLikePredicate";

    /**
     * Whether this datastore supports "SELECT a.* FROM (SELECT * FROM TBL1 INNER JOIN TBL2 ON tbl1.x = tbl2.y ) a"
     * If the database does not support the SQL statement generated is like 
     * "SELECT a.* FROM (TBL1 INNER JOIN TBL2 ON tbl1.x = tbl2.y ) a"
     */
    public static final String PROJECTION_IN_TABLE_REFERENCE_JOINS = "ProjectionInTableReferenceJoins";

    /**
     * Accessor for whether the SQL extensions CUBE, ROLLUP are supported.
     * TODO Move this to a plugin extension as cube and rollup options (separate)
     */
    public static final String ANALYSIS_METHODS = "AnalysisMethods";

    /**
     * Whether this datastore supports the use of the catalog name in ORM table definitions (DDL).
     */
    public static final String CATALOGS_IN_TABLE_DEFINITIONS = "CatalogInTableDefinition";

    /**
     * Whether this datastore supports the use of the schema name in ORM table definitions (DDL).
     */
    public static final String SCHEMAS_IN_TABLE_DEFINITIONS = "SchemaInTableDefinition";

    public static final String IDENTIFIERS_LOWERCASE = "LowerCaseIdentifiers";
    public static final String IDENTIFIERS_MIXEDCASE = "MixedCaseIdentifiers";
    public static final String IDENTIFIERS_UPPERCASE = "UpperCaseIdentifiers";
    public static final String IDENTIFIERS_LOWERCASE_QUOTED = "LowerCaseQuotedIdentifiers";
    public static final String IDENTIFIERS_MIXEDCASE_QUOTED = "MixedCaseQuotedIdentifiers";
    public static final String IDENTIFIERS_UPPERCASE_QUOTED = "UpperCaseQuotedIdentifiers";
    public static final String IDENTIFIERS_MIXEDCASE_SENSITIVE = "MixedCaseSensitiveIdentifiers";
    public static final String IDENTIFIERS_MIXEDCASE_QUOTED_SENSITIVE = "MixedCaseQuotedSensitiveIdentifiers";

    /**
     * Accessor for the options that are supported by this datastore adapter and the underlying datastore.
     * @return The options (Collection<String>)
     */
    Collection<String> getSupportedOptions();

    /**
     * Accessor for whether the supplied option is supported.
     * @param option The option
     * @return Whether supported.
     */
    boolean supportsOption(String option);

    /**
     * Accessor for a Mapping Manager suitable for use with this datastore adapter.
     * @param storeMgr The StoreManager
     * @return The Mapping Manager.
     */
    MappingManager getMappingManager(MappedStoreManager storeMgr);

    /**
     * Accessor for the Vendor ID for this datastore.
     * @return Vendor id for this datastore
     */
    String getVendorID();

    /**
     * Initialise the types for this datastore.
     * @param handler SchemaHandler that we initialise the types for
     * @param mconn Managed connection to use
     */
    void initialiseTypes(StoreSchemaHandler handler, ManagedConnection mconn);

    /**
     * Set any properties controlling how the adapter is configured.
     * @param props The properties
     */
    void setProperties(Map<String, Object> props);

    /**
     * Remove all mappings from the mapping manager that don't have a datastore type initialised.
     * @param handler Schema handler
     * @param mconn Managed connection to use
     */
    void removeUnsupportedMappings(StoreSchemaHandler handler, ManagedConnection mconn);

    /**
     * Method to check if a word is reserved for this datastore.
     * @param word The word
     * @return Whether it is reserved
     */
    boolean isReservedKeyword(String word);

    /**
     * Creates the auxiliary functions/procedures in the datastore 
     * @param conn the connection to the datastore
     */
    void initialiseDatastore(Object conn);

    /**
     * Accessor for the quote string to use when quoting identifiers.
     * @return The quote string for the identifier
     */
    String getIdentifierQuoteString();

    /**
     * Accessor for the catalog separator (string to separate the catalog/schema and the identifier).
     * @return Catalog separator string.
     */
    String getCatalogSeparator();

    /**
     * Utility to return the adapter time in case there are rounding issues with millisecs etc.
     * @param time The timestamp
     * @return The time in millisecs
     */
    long getAdapterTime(Timestamp time);

    /**
     * Accessor for the datastore product name.
     * @return product name
     */
    String getDatastoreProductName();

    /**
     * Accessor for the datastore product version.
     * @return product version
     */
    String getDatastoreProductVersion();

    /**
     * Accessor for the datastore driver name.
     * @return product name
     */
    String getDatastoreDriverName();

    /**
     * Accessor for the datastore driver version.
     * @return driver version
     */
    String getDatastoreDriverVersion();

    /**
     * Verifies if the given <code>columnDef</code> is an identity field type for the datastore.
     * @param columnDef the datastore type name
     * @return true when the <code>columnDef</code> has values for identity generation in the datastore
     **/
    boolean isIdentityFieldDataType(String columnDef);

    /**
     * Method to return the maximum length of a datastore identifier of the specified type.
     * If no limit exists then returns -1
     * @param identifierType Type of identifier
     * @return The max permitted length of this type of identifier
     */
    int getDatastoreIdentifierMaxLength(IdentifierType identifierType);

    /**
     * Accessor for the maximum foreign keys by table permitted in this datastore.
     * @return Max number of foreign keys
     */
    int getMaxForeignKeys();
    
    /**
     * Accessor for the maximum indexes by table permitted in this datastore.
     * @return Max number of indices
     */
    int getMaxIndexes();

    /**
     * Whether the datastore will support setting the query fetch size to the supplied value.
     * @param size The value to set to
     * @return Whether it is supported.
     */
    boolean supportsQueryFetchSize(int size);

    /**
     * Method to return this object as a string.
     * @return String version of this object.
     */
    String toString();
}