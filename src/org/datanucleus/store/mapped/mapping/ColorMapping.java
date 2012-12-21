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
package org.datanucleus.store.mapped.mapping;

import java.awt.Color;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ClassNameConstants;
import org.datanucleus.ExecutionContext;
import org.datanucleus.NucleusContext;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.store.mapped.DatastoreContainerObject;
import org.datanucleus.store.mapped.MappedStoreManager;

/**
 * Mapping for java.awt.Color mapping the red, green, blue and alpha values to datastore fields.
 */
public class ColorMapping extends SingleFieldMultiMapping
{
    /**
     * Initialise this JavaTypeMapping with the given DatastoreAdapter for the given field/property MetaData.
     * @param mmd Metadata for the field/property to be mapped (if any)
     * @param container The datastore container storing this mapping (if any)
     * @param clr the ClassLoaderResolver
     */
    public void initialize(AbstractMemberMetaData mmd, DatastoreContainerObject container, ClassLoaderResolver clr)
    {
		super.initialize(mmd, container, clr);
		addDatastoreFields();
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.mapped.mapping.JavaTypeMapping#initialize(org.datanucleus.store.mapped.MappedStoreManager, java.lang.String)
     */
    public void initialize(MappedStoreManager storeMgr, String type)
    {
        super.initialize(storeMgr, type);
        addDatastoreFields();
    }

    protected void addDatastoreFields()
    {
        addDatastoreField(ClassNameConstants.INT); // Red
        addDatastoreField(ClassNameConstants.INT); // Green
        addDatastoreField(ClassNameConstants.INT); // Blue
        addDatastoreField(ClassNameConstants.INT); // Alpha
    }
    /* (non-Javadoc)
     * @see org.datanucleus.store.mapping.JavaTypeMapping#getJavaType()
     */
    public Class getJavaType()
    {
        return Color.class;
    }

    /**
     * Method to return the value to be stored in the specified datastore index given the overall
     * value for this java type.
     * @param index The datastore index
     * @param value The overall value for this java type
     * @return The value for this datastore index
     */
    public Object getValueForDatastoreMapping(NucleusContext nucleusCtx, int index, Object value)
    {
        if (index == 0)
        {
            return ((Color)value).getRed();
        }
        else if (index == 1)
        {
            return ((Color)value).getRed();
        }
        else if (index == 2)
        {
            return ((Color)value).getRed();
        }
        else if (index == 3)
        {
            return ((Color)value).getRed();
        }
        throw new IndexOutOfBoundsException();
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.mapping.JavaTypeMapping#setObject(org.datanucleus.ExecutionContext, java.lang.Object, int[], java.lang.Object)
     */
    public void setObject(ExecutionContext ec, Object preparedStatement, int[] exprIndex, Object value)
    {
        Color color = (Color) value;
        if (color == null)
        {
            getDatastoreMapping(0).setObject(preparedStatement, exprIndex[0], null);
            getDatastoreMapping(1).setObject(preparedStatement, exprIndex[1], null);
            getDatastoreMapping(2).setObject(preparedStatement, exprIndex[2], null);
            getDatastoreMapping(3).setObject(preparedStatement, exprIndex[3], null);
        }
        else
        {
            getDatastoreMapping(0).setInt(preparedStatement,exprIndex[0],color.getRed());
            getDatastoreMapping(1).setInt(preparedStatement,exprIndex[1],color.getGreen());
            getDatastoreMapping(2).setInt(preparedStatement,exprIndex[2],color.getBlue());
            getDatastoreMapping(3).setInt(preparedStatement,exprIndex[3],color.getAlpha());
        }
    }
    
    /* (non-Javadoc)
     * @see org.datanucleus.store.mapping.JavaTypeMapping#getObject(org.datanucleus.ExecutionContext, java.lang.Object, int[])
     */
    public Object getObject(ExecutionContext ec, Object resultSet, int[] exprIndex)
    {
        try
        {
            // Check for null entries
            if (getDatastoreMapping(0).getObject(resultSet, exprIndex[0]) == null)
            {
                return null;
            }
        }
        catch (Exception e)
        {
            // Do nothing
        }

        int red = getDatastoreMapping(0).getInt(resultSet,exprIndex[0]); 
        int green = getDatastoreMapping(1).getInt(resultSet,exprIndex[1]); 
        int blue = getDatastoreMapping(2).getInt(resultSet,exprIndex[2]); 
        int alpha = getDatastoreMapping(3).getInt(resultSet,exprIndex[3]);
        return new Color(red,green,blue,alpha);
    }
}