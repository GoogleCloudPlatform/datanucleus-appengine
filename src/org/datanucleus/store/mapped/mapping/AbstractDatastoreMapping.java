/**********************************************************************
Copyright (c) 2009 Andy Jefferson and others. All rights reserved.
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

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.util.Localiser;

/**
 * Abstract base for datastore mappings.
 */
public abstract class AbstractDatastoreMapping implements DatastoreMapping
{
    protected static final Localiser LOCALISER = Localiser.getInstance(
        "org.datanucleus.Localisation", org.datanucleus.ClassConstants.NUCLEUS_CONTEXT_LOADER);

    /** Mapping of the Java type. */
    protected final JavaTypeMapping mapping;

    public AbstractDatastoreMapping(JavaTypeMapping mapping)
    {
        this.mapping = mapping;
        if (mapping != null)
        {
            // Register this datastore mapping with the owning JavaTypeMapping
            mapping.addDatastoreMapping(this);
        }
    }

    /**
     * Accessor for the java type mapping
     * @return The java type mapping used
     */
    public JavaTypeMapping getJavaTypeMapping()
    {
        return mapping;
    }

    public void setBoolean(Object preparedStatement, int exprIndex, boolean value)
    {
        throw new NucleusException(failureMessage("setBoolean")).setFatal();
    }

    public boolean getBoolean(Object resultSet, int exprIndex)
    {
        throw new NucleusException(failureMessage("getBoolean")).setFatal();
    }

    public void setChar(Object preparedStatement, int exprIndex, char value)
    {
        throw new NucleusException(failureMessage("setChar")).setFatal();
    }

    public char getChar(Object resultSet, int exprIndex)
    {
        throw new NucleusException(failureMessage("getChar")).setFatal();
    }

    public void setByte(Object preparedStatement, int exprIndex, byte value)
    {
        throw new NucleusException(failureMessage("setByte")).setFatal();
    }

    public byte getByte(Object resultSet, int exprIndex)
    {
        throw new NucleusException(failureMessage("getByte")).setFatal();
    }

    public void setShort(Object preparedStatement, int exprIndex, short value)
    {
        throw new NucleusException(failureMessage("setShort")).setFatal();
    }

    public short getShort(Object resultSet, int exprIndex)
    {
        throw new NucleusException(failureMessage("getShort")).setFatal();
    }

    public void setInt(Object preparedStatement, int exprIndex, int value)
    {
        throw new NucleusException(failureMessage("setInt")).setFatal();
    }

    public int getInt(Object resultSet, int exprIndex)
    {
        throw new NucleusException(failureMessage("getInt")).setFatal();
    }

    public void setLong(Object preparedStatement, int exprIndex, long value)
    {
        throw new NucleusException(failureMessage("setLong")).setFatal();
    }

    public long getLong(Object resultSet, int exprIndex)
    {
        throw new NucleusException(failureMessage("getLong")).setFatal();
    }

    public void setFloat(Object preparedStatement, int exprIndex, float value)
    {
        throw new NucleusException(failureMessage("setFloat")).setFatal();
    }

    public float getFloat(Object resultSet, int exprIndex)
    {
        throw new NucleusException(failureMessage("getFloat")).setFatal();
    }

    public void setDouble(Object preparedStatement, int exprIndex, double value)
    {
        throw new NucleusException(failureMessage("setDouble")).setFatal();
    }

    public double getDouble(Object resultSet, int exprIndex)
    {
        throw new NucleusException(failureMessage("getDouble")).setFatal();
    }

    public void setString(Object preparedStatement, int exprIndex, String value)
    {
        throw new NucleusException(failureMessage("setString")).setFatal();
    }

    public String getString(Object resultSet, int exprIndex)
    {
        throw new NucleusException(failureMessage("getString")).setFatal();
    }

    public void setObject(Object preparedStatement, int exprIndex, Object value)
    {
        throw new NucleusException(failureMessage("setObject")).setFatal();
    }

    public Object getObject(Object resultSet, int exprIndex)
    {
        throw new NucleusException(failureMessage("getObject")).setFatal();
    }

    /**
     * Accessor for whether the mapping is decimal-based.
     * @return Whether the mapping is decimal based
     */
    public boolean isDecimalBased()
    {
        return false;
    }

    /**
     * Accessor for whether the mapping is integer-based.
     * @return Whether the mapping is integer based
     */
    public boolean isIntegerBased()
    {
        return false;
    }

    /**
     * Accessor for whether the mapping is string-based.
     * @return Whether the mapping is string based
     */
    public boolean isStringBased()
    {
        return false;
    }

    /**
     * Accessor for whether the mapping is bit-based.
     * @return Whether the mapping is bit based
     */
    public boolean isBitBased()
    {
        return false;
    }

    /**
     * Accessor for whether the mapping is boolean-based.
     * @return Whether the mapping is boolean based
     */
    public boolean isBooleanBased()
    {
        return false;
    }

    /**
     * Utility to output any error message.
     * @param method The method that failed.
     * @return The localised failure message
     **/
    protected String failureMessage(String method)
    {
        return LOCALISER.msg("041005",getClass().getName(), method,
            mapping.getMemberMetaData().getFullFieldName());
    }
}