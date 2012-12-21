/**********************************************************************
Copyright (c) 2004 Andy Jefferson and others. All rights reserved. 
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
import org.datanucleus.store.mapped.mapping.DatastoreMapping;
import org.datanucleus.store.mapped.mapping.JavaTypeMapping;

/**
 * Representation of a Java field (component) in a datastore.
 * In the case of RDBMS this will be a column.
 * In the case of a file-based structure this may be a file.
 */ 
public interface DatastoreField
{
    /**
     * Accessor for the StoreManager for this table.
     * @return The StoreManager.
     */
    MappedStoreManager getStoreManager();

    /**
     * Accessor for the type of data stored in this field.
     * @return The type of data in the field.
     */
    String getStoredJavaType();

    /**
     * Mutator for the identifier of the column.
     * @param identifier The identifier
     */
    void setIdentifier(DatastoreIdentifier identifier);

    /**
     * Accessor for the identifier for this object.
     * @return The identifier.
     */
    DatastoreIdentifier getIdentifier();

    /**
     * Mutator to make the field the primary key.
     */
    void setAsPrimaryKey();

    /**
     * Accessor for whether the field is the primary key in the datastore.
     * @return whether the field is (part of) the primary key
     */
    boolean isPrimaryKey();

    /**
     * Mutator for the nullability of the datastore field.
     * @return The datastore field with the updated info
     */
    DatastoreField setNullable();

    /**
     * Accessor for whether the field is nullable in the datastore.
     * @return whether the field is nullable
     */
    boolean isNullable();

    /**
     * Mutator for the defaultability of the datastore field.
     * @return The datastore field with the updated info
     */
    DatastoreField setDefaultable(); 

    /**
     * Accessor for whether the column is defaultable.
     * @return whether the column is defaultable
     */
    boolean isDefaultable();

    /**
     * Mutator for the uniqueness of the column.
     * @return The datastore field with the updated info
     */
    DatastoreField setUnique();

    /**
     * Accessor for whether the column is unique.
     * @return whether the column is unique
     */
    boolean isUnique();

    /**
     * Mutator for whether we set this column as an identity column.
     * An "identity" column is typically treated differently in the datastore being
     * given a value by the datastore itself.
     * In RDBMS this would mean that the column is "AUTO_INCREMENT", "SERIAL" or 
     * @param identity True if column is identity
     */
    DatastoreField setIdentity(boolean identity);

    /**
     * Accessor for the whether this column is an identity column.
     * @return true if column is identity.
     */
    boolean isIdentity();

    /**
     * Mutator for the default Value
     * @param object default value
     */
    void setDefaultValue(Object object);

    /**
     * Accessor for the default Value
     * @return the default value
     */
    Object getDefaultValue();

    /**
     * Method to associate this datastore field with its mapping.
     * @param mapping The mapping for this datastore field
     */
    void setDatastoreMapping(DatastoreMapping mapping);

    /**
     * Accessor for the datastore mapping that this datastore field relates to.
     * @return The datastore mapping
     */
    DatastoreMapping getDatastoreMapping();

    /**
     * Method to set the MetaData for this datastore field.
     * Should only be called before completion of initialisation.
     * @param md The MetaData
     */
    void setColumnMetaData(ColumnMetaData md);

    /**
     * Access the metadata definition defining this DatastoreField.
     * @return the MetaData
     */
    ColumnMetaData getColumnMetaData();

    /**
     * Accessor for the JavaTypeMapping for the field/property that owns this column.
     * @return The JavaTypeMapping
     */
    JavaTypeMapping getJavaTypeMapping();
    
    /**
     * Accessor for the DatastoreContainerObject container of this field
     * @return The DatastoreContainerObject
     */
    DatastoreContainerObject getDatastoreContainerObject();
    
    /**
     * Wraps the column name with a FUNCTION.
     * <PRE>example: SQRT(?) generates: SQRT(columnName)</PRE>
     * @param replacementValue the replacement to ?. Probably it's a column name, that may be fully qualified name or not
     * @return a String with function taking as parameter the replacementValue
     */
    String applySelectFunction(String replacementValue);

    /**
     * Copy the configuration of this field to another field
     * @param col the datastore field
     */
	void copyConfigurationTo(DatastoreField col);

    /**
     * Accessor for the MetaData of the field/property that this is the datastore field for.
     * @return MetaData of the field/property (if representing a field/property of a class).
     */
    AbstractMemberMetaData getMemberMetaData();
}