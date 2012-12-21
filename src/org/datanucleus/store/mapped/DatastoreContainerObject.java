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

import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.DiscriminatorMetaData;
import org.datanucleus.metadata.VersionMetaData;
import org.datanucleus.store.mapped.mapping.JavaTypeMapping;

/**
 * This represents a container of fields. Such as table (container of columns), file, etc.
 */
public interface DatastoreContainerObject
{
    /**
     * Accessor for the StoreManager for this table.
     * @return The StoreManager.
     */
    MappedStoreManager getStoreManager();

    /**
     * Accessor for the identifier for this object.
     * @return The identifier.
     */
    DatastoreIdentifier getIdentifier();

    /**
     * Method to add a new column to the internal representation.
     * @param storedJavaType The type of the Java field to store
     * @param name The name of the column
     * @param mapping The type mapping for this column
     * @param colmd The column MetaData
     * @return The new Column
     */
    DatastoreField addDatastoreField(String storedJavaType, DatastoreIdentifier name, JavaTypeMapping mapping, 
            ColumnMetaData colmd);
    
    /**
     * Checks if there is a DatastoreField for the identifier
     * @param identifier the identifier of the DatastoreField
     * @return true if the DatastoreField exists for the identifier
     */
    boolean hasDatastoreField(DatastoreIdentifier identifier);    

    /**
     * Accessor for the Datastore field with the specified identifier.
     * Returns null if has no column of this name.
     * @param identifier The name of the column
     * @return The column
     */
    DatastoreField getDatastoreField(DatastoreIdentifier identifier);

    /**
     * Accessor for the DatastoreFields for this table.
     * @return the DatastoreField[]
     */
    DatastoreField[] getDatastoreFields();

    /**
     * Accessor for the ID mapping of this container object.
     * @return The ID Mapping (if present)
     */
    JavaTypeMapping getIdMapping();

    /**
     * Accessor for the mapping for the specified FieldMetaData. 
     * A datastore container object may store many fields.
     * @param mmd Metadata for the field/property
     * @return The Mapping for the member, or null if the FieldMetaData cannot be found
     */
    JavaTypeMapping getMemberMapping(AbstractMemberMetaData mmd);

    /**
     * Accessor for Discriminator MetaData.
     * @return Returns the Discriminator MetaData.
     */
    DiscriminatorMetaData getDiscriminatorMetaData();

    /**
     * Accessor for the discriminator mapping specified.
     * @param allowSuperclasses Whether we should return just the mapping from this table
     *     or whether we should return it when this table has none and the supertable has
     * @return The discriminator mapping
     */
    JavaTypeMapping getDiscriminatorMapping(boolean allowSuperclasses);

    /**
     * Accessor for the multi-tenancy mapping (if any).
     * @return The multi-tenancy mapping
     */
    JavaTypeMapping getMultitenancyMapping();

    /**
     * Accessor for the Version MetaData.
     * @return Returns the Version MetaData.
     */
    VersionMetaData getVersionMetaData();

    /**
     * Accessor for the version mapping.
     * @param allowSuperclasses Whether we should return just the mapping from this table
     *     or whether we should return it when this table has none and the supertable has
     * @return The version mapping.
     */
    JavaTypeMapping getVersionMapping(boolean allowSuperclasses);
}