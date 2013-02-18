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

import java.util.Collection;

import org.datanucleus.store.mapped.mapping.MappingManager;

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
     * Method to return this object as a string.
     * @return String version of this object.
     */
    String toString();
}