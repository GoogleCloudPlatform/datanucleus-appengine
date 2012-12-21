/**********************************************************************
Copyright (c) 2008 Andy Jefferson and others. All rights reserved.
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
package org.datanucleus.store.mapped.exceptions;

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.mapped.DatastoreField;
import org.datanucleus.util.Localiser;

/**
 * A <tt>DuplicateDatastoreFieldException</tt> is thrown if an attempt is made to
 * add a column to a table with a name already in-use by an existing column.
 */
public class DuplicateDatastoreFieldException extends NucleusException
{
    private static final Localiser LOCALISER=Localiser.getInstance(
        "org.datanucleus.Localisation", org.datanucleus.ClassConstants.NUCLEUS_CONTEXT_LOADER);

    /** Column that cannot be created because it conflicts with existing column with same identifier. */
    private DatastoreField conflictingColumn;

    /**
     * Constructs a duplicate column name exception.
     * @param tableName Name of the table being initialized.
     * @param col1 Column we already have
     * @param col2 Column that we tried to create
     */
    public DuplicateDatastoreFieldException(String tableName, DatastoreField col1, DatastoreField col2)
    {
        super(LOCALISER.msg("020007", col1.getIdentifier(), tableName,
            col1.getMemberMetaData() == null ?
                LOCALISER.msg("020008") :
                (col1.getMemberMetaData() != null ? col1.getMemberMetaData().getFullFieldName() : null),
            col2.getMemberMetaData() == null ?
                LOCALISER.msg("020008") :
                (col2.getMemberMetaData() != null ? col2.getMemberMetaData().getFullFieldName() : null)));
        this.conflictingColumn = col2;
        setFatal();
    }

    /**
     * Accessor for the column that could not be created because it conflicts with something already present.
     * @return The column
     */
    public DatastoreField getConflictingColumn()
    {
        return conflictingColumn;
    }
}