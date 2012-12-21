/**********************************************************************
Copyright (c) 2012 Andy Jefferson and others. All rights reserved.
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
package org.datanucleus.store.mapped.mapping;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.store.mapped.DatastoreContainerObject;
import org.datanucleus.store.types.TypeManager;
import org.datanucleus.store.types.converters.TypeConverter;

/**
 * Mapping where the member has its value converted to/from some storable datastore type using a TypeConverter.
 */
public class TypeConverterMapping extends SingleFieldMapping
{
    TypeConverter converter;

    /* (non-Javadoc)
     * @see org.datanucleus.store.mapped.mapping.SingleFieldMapping#initialize(org.datanucleus.metadata.AbstractMemberMetaData, org.datanucleus.store.mapped.DatastoreContainerObject, org.datanucleus.ClassLoaderResolver)
     */
    @Override
    public void initialize(AbstractMemberMetaData fmd, DatastoreContainerObject container, ClassLoaderResolver clr)
    {
        if (fmd.getTypeConverterName() != null)
        {
            converter = container.getStoreManager().getNucleusContext().getTypeManager().getTypeConverterForName(fmd.getTypeConverterName());
        }
        else
        {
            throw new NucleusException("Attempt to create TypeConverterMapping when no type converter defined for member " + fmd.getFullFieldName());
        }

        super.initialize(fmd, container, clr);
    }

    /**
     * Accessor for the name of the java-type actually used when mapping the particular datastore
     * field. This java-type must have an entry in the datastore mappings.
     * @param index requested datastore field index.
     * @return the name of java-type for the requested datastore field.
     */
    public String getJavaTypeForDatastoreMapping(int index)
    {
        return TypeManager.getDatastoreTypeForTypeConverter(converter, getJavaType()).getName();
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.mapped.mapping.JavaTypeMapping#getJavaType()
     */
    @Override
    public Class getJavaType()
    {
        return mmd.getType();
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.mapped.mapping.SingleFieldMapping#setBoolean(org.datanucleus.store.ExecutionContext, java.lang.Object, int[], boolean)
     */
    @Override
    public void setBoolean(ExecutionContext ec, Object preparedStatement, int[] exprIndex, boolean value)
    {
        if (exprIndex == null)
        {
            return;
        }

        getDatastoreMapping(0).setBoolean(preparedStatement, exprIndex[0], (Boolean)converter.toDatastoreType(value));
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.mapped.mapping.SingleFieldMapping#getBoolean(org.datanucleus.store.ExecutionContext, java.lang.Object, int[])
     */
    @Override
    public boolean getBoolean(ExecutionContext ec, Object resultSet, int[] exprIndex)
    {
        if (exprIndex == null)
        {
            return false;
        }

        Boolean datastoreValue = getDatastoreMapping(0).getBoolean(resultSet, exprIndex[0]);
        return (datastoreValue != null ? (Boolean)converter.toMemberType(datastoreValue) : null);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.mapped.mapping.SingleFieldMapping#setByte(org.datanucleus.store.ExecutionContext, java.lang.Object, int[], byte)
     */
    @Override
    public void setByte(ExecutionContext ec, Object preparedStatement, int[] exprIndex, byte value)
    {
        if (exprIndex == null)
        {
            return;
        }

        getDatastoreMapping(0).setByte(preparedStatement, exprIndex[0], (Byte)converter.toDatastoreType(value));
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.mapped.mapping.SingleFieldMapping#getByte(org.datanucleus.store.ExecutionContext, java.lang.Object, int[])
     */
    @Override
    public byte getByte(ExecutionContext ec, Object resultSet, int[] exprIndex)
    {
        if (exprIndex == null)
        {
            return 0;
        }

        Byte datastoreValue = getDatastoreMapping(0).getByte(resultSet, exprIndex[0]);
        return (datastoreValue != null ? (Byte)converter.toMemberType(datastoreValue) : null);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.mapped.mapping.SingleFieldMapping#setChar(org.datanucleus.store.ExecutionContext, java.lang.Object, int[], char)
     */
    @Override
    public void setChar(ExecutionContext ec, Object preparedStatement, int[] exprIndex, char value)
    {
        if (exprIndex == null)
        {
            return;
        }

        getDatastoreMapping(0).setChar(preparedStatement, exprIndex[0], (Character)converter.toDatastoreType(value));
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.mapped.mapping.SingleFieldMapping#getChar(org.datanucleus.store.ExecutionContext, java.lang.Object, int[])
     */
    @Override
    public char getChar(ExecutionContext ec, Object resultSet, int[] exprIndex)
    {
        if (exprIndex == null)
        {
            return 0;
        }

        Character datastoreValue = getDatastoreMapping(0).getChar(resultSet, exprIndex[0]);
        return (datastoreValue != null ? (Character)converter.toMemberType(datastoreValue) : null);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.mapped.mapping.SingleFieldMapping#setDouble(org.datanucleus.store.ExecutionContext, java.lang.Object, int[], double)
     */
    @Override
    public void setDouble(ExecutionContext ec, Object preparedStatement, int[] exprIndex, double value)
    {
        if (exprIndex == null)
        {
            return;
        }

        getDatastoreMapping(0).setDouble(preparedStatement, exprIndex[0], (Double)converter.toDatastoreType(value));
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.mapped.mapping.SingleFieldMapping#getDouble(org.datanucleus.store.ExecutionContext, java.lang.Object, int[])
     */
    @Override
    public double getDouble(ExecutionContext ec, Object resultSet, int[] exprIndex)
    {
        if (exprIndex == null)
        {
            return 0;
        }

        Double datastoreValue = getDatastoreMapping(0).getDouble(resultSet, exprIndex[0]);
        return (datastoreValue != null ? (Double)converter.toMemberType(datastoreValue) : null);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.mapped.mapping.SingleFieldMapping#setFloat(org.datanucleus.store.ExecutionContext, java.lang.Object, int[], float)
     */
    @Override
    public void setFloat(ExecutionContext ec, Object preparedStatement, int[] exprIndex, float value)
    {
        if (exprIndex == null)
        {
            return;
        }

        getDatastoreMapping(0).setFloat(preparedStatement, exprIndex[0], (Float)converter.toDatastoreType(value));
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.mapped.mapping.SingleFieldMapping#getFloat(org.datanucleus.store.ExecutionContext, java.lang.Object, int[])
     */
    @Override
    public float getFloat(ExecutionContext ec, Object resultSet, int[] exprIndex)
    {
        if (exprIndex == null)
        {
            return 0;
        }

        Float datastoreValue = getDatastoreMapping(0).getFloat(resultSet, exprIndex[0]);
        return (datastoreValue != null ? (Float)converter.toMemberType(datastoreValue) : null);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.mapped.mapping.SingleFieldMapping#setInt(org.datanucleus.store.ExecutionContext, java.lang.Object, int[], int)
     */
    @Override
    public void setInt(ExecutionContext ec, Object preparedStatement, int[] exprIndex, int value)
    {
        if (exprIndex == null)
        {
            return;
        }

        getDatastoreMapping(0).setInt(preparedStatement, exprIndex[0], (Integer)converter.toDatastoreType(value));
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.mapped.mapping.SingleFieldMapping#getInt(org.datanucleus.store.ExecutionContext, java.lang.Object, int[])
     */
    @Override
    public int getInt(ExecutionContext ec, Object resultSet, int[] exprIndex)
    {
        if (exprIndex == null)
        {
            return 0;
        }

        Integer datastoreValue = getDatastoreMapping(0).getInt(resultSet, exprIndex[0]);
        return (datastoreValue != null ? (Integer)converter.toMemberType(datastoreValue) : null);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.mapped.mapping.SingleFieldMapping#setLong(org.datanucleus.store.ExecutionContext, java.lang.Object, int[], long)
     */
    @Override
    public void setLong(ExecutionContext ec, Object preparedStatement, int[] exprIndex, long value)
    {
        if (exprIndex == null)
        {
            return;
        }

        getDatastoreMapping(0).setLong(preparedStatement, exprIndex[0], (Long)converter.toDatastoreType(value));
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.mapped.mapping.SingleFieldMapping#getLong(org.datanucleus.store.ExecutionContext, java.lang.Object, int[])
     */
    @Override
    public long getLong(ExecutionContext ec, Object resultSet, int[] exprIndex)
    {
        if (exprIndex == null)
        {
            return 0;
        }

        Long datastoreValue = getDatastoreMapping(0).getLong(resultSet, exprIndex[0]);
        return (datastoreValue != null ? (Long)converter.toMemberType(datastoreValue) : null);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.mapped.mapping.SingleFieldMapping#setShort(org.datanucleus.store.ExecutionContext, java.lang.Object, int[], short)
     */
    @Override
    public void setShort(ExecutionContext ec, Object preparedStatement, int[] exprIndex, short value)
    {
        if (exprIndex == null)
        {
            return;
        }

        getDatastoreMapping(0).setShort(preparedStatement, exprIndex[0], (Short)converter.toDatastoreType(value));
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.mapped.mapping.SingleFieldMapping#getShort(org.datanucleus.store.ExecutionContext, java.lang.Object, int[])
     */
    @Override
    public short getShort(ExecutionContext ec, Object resultSet, int[] exprIndex)
    {
        if (exprIndex == null)
        {
            return 0;
        }

        Short datastoreValue = getDatastoreMapping(0).getShort(resultSet, exprIndex[0]);
        return (datastoreValue != null ? (Short)converter.toMemberType(datastoreValue) : null);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.mapped.mapping.SingleFieldMapping#setString(org.datanucleus.store.ExecutionContext, java.lang.Object, int[], java.lang.String)
     */
    @Override
    public void setString(ExecutionContext ec, Object preparedStatement, int[] exprIndex, String value)
    {
        if (exprIndex == null)
        {
            return;
        }

        getDatastoreMapping(0).setString(preparedStatement, exprIndex[0], (String)converter.toDatastoreType(value));
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.mapped.mapping.SingleFieldMapping#getString(org.datanucleus.store.ExecutionContext, java.lang.Object, int[])
     */
    @Override
    public String getString(ExecutionContext ec, Object resultSet, int[] exprIndex)
    {
        if (exprIndex == null)
        {
            return null;
        }

        String datastoreValue = getDatastoreMapping(0).getString(resultSet, exprIndex[0]);
        return (datastoreValue != null ? (String)converter.toMemberType(datastoreValue) : null);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.mapped.mapping.SingleFieldMapping#setObject(org.datanucleus.store.ExecutionContext, java.lang.Object, int[], java.lang.Object)
     */
    @Override
    public void setObject(ExecutionContext ec, Object preparedStatement, int[] exprIndex, Object value)
    {
        if (exprIndex == null)
        {
            return;
        }

        getDatastoreMapping(0).setObject(preparedStatement, exprIndex[0], converter.toDatastoreType(value));
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.mapped.mapping.SingleFieldMapping#getObject(org.datanucleus.store.ExecutionContext, java.lang.Object, int[])
     */
    @Override
    public Object getObject(ExecutionContext ec, Object resultSet, int[] exprIndex)
    {
        if (exprIndex == null)
        {
            return null;
        }

        Object datastoreValue = getDatastoreMapping(0).getObject(resultSet, exprIndex[0]);
        return (datastoreValue != null ? converter.toMemberType(datastoreValue) : null);
    }
}