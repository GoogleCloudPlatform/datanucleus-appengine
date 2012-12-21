/**********************************************************************
Copyright (c) 2004 Erik Bengtson and others. All rights reserved.
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

import org.datanucleus.store.mapped.DatastoreField;

/**
 * Representation of the mapping of a datastore type.
 */
public interface DatastoreMapping
{
    /**
     * Whether the field mapped is nullable.
     * @return true if is nullable
     */
    boolean isNullable();

    /**
     * The datastore field mapped.
     * @return the DatastoreField
     */
    DatastoreField getDatastoreField();

    /**
     * The mapping for the java type that this datastore mapping is used by.
     * This will return null if this simply maps a datastore field in the datastore and has
     * no associated java type in a class.
     * @return the JavaTypeMapping
     */
    JavaTypeMapping getJavaTypeMapping();

    /**
     * Accessor for whether the mapping is decimal-based.
     * @return Whether the mapping is decimal based
     */
    boolean isDecimalBased();

    /**
     * Accessor for whether the mapping is integer-based.
     * @return Whether the mapping is integer based
     */
    boolean isIntegerBased();

    /**
     * Accessor for whether the mapping is string-based.
     * @return Whether the mapping is string based
     */
    boolean isStringBased();

    /**
     * Accessor for whether the mapping is bit-based.
     * @return Whether the mapping is bit based
     */
    boolean isBitBased();

    /**
     * Accessor for whether the mapping is boolean-based.
     * @return Whether the mapping is boolean based
     */
    boolean isBooleanBased();

    /**
     * Sets a <code>value</code> into <code>preparedStatement</code> 
     * at position specified by <code>paramIndex</code>. 
     * @param preparedStatement a datastore object that executes statements in the database 
     * @param paramIndex the position of the value in the statement
     * @param value the value
     */
    void setBoolean(Object preparedStatement, int paramIndex, boolean value);

    /**
     * Sets a <code>value</code> into <code>preparedStatement</code> 
     * at position specified by <code>paramIndex</code>. 
     * @param preparedStatement a datastore object that executes statements in the database 
     * @param paramIndex the position of the value in the statement
     * @param value the value
     */
    void setChar(Object preparedStatement, int paramIndex, char value);

    /**
     * Sets a <code>value</code> into <code>preparedStatement</code> 
     * at position specified by <code>paramIndex</code>. 
     * @param preparedStatement a datastore object that executes statements in the database 
     * @param paramIndex the position of the value in the statement
     * @param value the value
     */
    void setByte(Object preparedStatement, int paramIndex, byte value);

    /**
     * Sets a <code>value</code> into <code>preparedStatement</code> 
     * at position specified by <code>paramIndex</code>. 
     * @param preparedStatement a datastore object that executes statements in the database 
     * @param paramIndex the position of the value in the statement
     * @param value the value
     */
    void setShort(Object preparedStatement, int paramIndex, short value);

    /**
     * Sets a <code>value</code> into <code>preparedStatement</code> 
     * at position specified by <code>paramIndex</code>. 
     * @param preparedStatement a datastore object that executes statements in the database 
     * @param paramIndex the position of the value in the statement
     * @param value the value
     */
    void setInt(Object preparedStatement, int paramIndex, int value);

    /**
     * Sets a <code>value</code> into <code>preparedStatement</code> 
     * at position specified by <code>paramIndex</code>. 
     * @param preparedStatement a datastore object that executes statements in the database 
     * @param paramIndex the position of the value in the statement
     * @param value the value
     */
    void setLong(Object preparedStatement, int paramIndex, long value);

    /**
     * Sets a <code>value</code> into <code>preparedStatement</code> 
     * at position specified by <code>paramIndex</code>. 
     * @param preparedStatement a datastore object that executes statements in the database 
     * @param paramIndex the position of the value in the statement
     * @param value the value
     */
    void setFloat(Object preparedStatement, int paramIndex, float value);

    /**
     * Sets a <code>value</code> into <code>preparedStatement</code> 
     * at position specified by <code>paramIndex</code>. 
     * @param preparedStatement a datastore object that executes statements in the database 
     * @param paramIndex the position of the value in the statement
     * @param value the value
     */
    void setDouble(Object preparedStatement, int paramIndex, double value);

    /**
     * Sets a <code>value</code> into <code>preparedStatement</code> 
     * at position specified by <code>paramIndex</code>. 
     * @param preparedStatement a datastore object that executes statements in the database 
     * @param paramIndex the position of the value in the statement
     * @param value the value
     */
    void setString(Object preparedStatement, int paramIndex, String value);

    /**
     * Sets a <code>value</code> into <code>preparedStatement</code> 
     * at position specified by <code>paramIndex</code>. 
     * @param preparedStatement a datastore object that executes statements in the database 
     * @param paramIndex the position of the value in the statement
     * @param value the value
     */
    void setObject(Object preparedStatement, int paramIndex, Object value);

    /**
     * Obtains a value from <code>resultSet</code> 
     * at position specified by <code>exprIndex</code>. 
     * @param resultSet an object returned from the datastore with values 
     * @param exprIndex the position of the value in the result
     * @return the value
     */
    boolean getBoolean(Object resultSet, int exprIndex);

    /**
     * Obtains a value from <code>resultSet</code> 
     * at position specified by <code>exprIndex</code>. 
     * @param resultSet an object returned from the datastore with values 
     * @param exprIndex the position of the value in the result
     * @return the value
     */
    char getChar(Object resultSet, int exprIndex);

    /**
     * Obtains a value from <code>resultSet</code> 
     * at position specified by <code>exprIndex</code>. 
     * @param resultSet an object returned from the datastore with values 
     * @param exprIndex the position of the value in the result
     * @return the value
     */
    byte getByte(Object resultSet, int exprIndex);

    /**
     * Obtains a value from <code>resultSet</code> 
     * at position specified by <code>exprIndex</code>. 
     * @param resultSet an object returned from the datastore with values 
     * @param exprIndex the position of the value in the result
     * @return the value
     */
    short getShort(Object resultSet, int exprIndex);

    /**
     * Obtains a value from <code>resultSet</code> 
     * at position specified by <code>exprIndex</code>. 
     * @param resultSet an object returned from the datastore with values 
     * @param exprIndex the position of the value in the result
     * @return the value
     */
    int getInt(Object resultSet, int exprIndex);

    /**
     * Obtains a value from <code>resultSet</code> 
     * at position specified by <code>exprIndex</code>. 
     * @param resultSet an object returned from the datastore with values 
     * @param exprIndex the position of the value in the result
     * @return the value
     */
    long getLong(Object resultSet, int exprIndex);

    /**
     * Obtains a value from <code>resultSet</code> 
     * at position specified by <code>exprIndex</code>. 
     * @param resultSet an object returned from the datastore with values 
     * @param exprIndex the position of the value in the result
     * @return the value
     */
    float getFloat(Object resultSet, int exprIndex);

    /**
     * Obtains a value from <code>resultSet</code> 
     * at position specified by <code>exprIndex</code>. 
     * @param resultSet an object returned from the datastore with values 
     * @param exprIndex the position of the value in the result
     * @return the value
     */
    double getDouble(Object resultSet, int exprIndex);

    /**
     * Obtains a value from <code>resultSet</code> 
     * at position specified by <code>exprIndex</code>. 
     * @param resultSet an object returned from the datastore with values 
     * @param exprIndex the position of the value in the result
     * @return the value
     */
    String getString(Object resultSet, int exprIndex);

    /**
     * Obtains a value from <code>resultSet</code> 
     * at position specified by <code>exprIndex</code>. 
     * @param resultSet an object returned from the datastore with values 
     * @param exprIndex the position of the value in the result
     * @return the value
     */
    Object getObject(Object resultSet, int exprIndex);
}