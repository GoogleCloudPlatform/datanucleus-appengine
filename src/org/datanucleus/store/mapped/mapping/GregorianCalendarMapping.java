/**********************************************************************
Copyright (c) 2005 Andy Jefferson and others. All rights reserved.
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

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ClassNameConstants;
import org.datanucleus.ExecutionContext;
import org.datanucleus.NucleusContext;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.store.mapped.DatastoreContainerObject;
import org.datanucleus.store.mapped.MappedStoreManager;

/**
 * Maps the class fields of a GregorianCalendar to datastore field(s).
 * JPOX traditionally supported this as mapping to 2 datastore fields (timestamp millisecs and timezone).
 * Here we also support it as mapping to a single datastore field (timestamp).
 */
public class GregorianCalendarMapping extends SingleFieldMultiMapping
{
    boolean singleColumn = false;

    /* (non-Javadoc)
     * @see org.datanucleus.store.mapping.JavaTypeMapping#initialize(AbstractMemberMetaData, DatastoreContainerObject, ClassLoaderResolver)
     */
    public void initialize(AbstractMemberMetaData fmd, DatastoreContainerObject container, ClassLoaderResolver clr)
    {
        super.initialize(fmd, container, clr);
        addDatastoreFields();
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.mapped.mapping.JavaTypeMapping#initialize(MappedStoreManager, java.lang.String)
     */
    public void initialize(MappedStoreManager storeMgr, String type)
    {
        super.initialize(storeMgr, type);
        addDatastoreFields();
    }

    protected void addDatastoreFields()
    {
        if (mmd!= null && mmd.hasExtension("calendar-one-column") && 
            mmd.getValueForExtension("calendar-one-column").equals("true"))
        {
            // If this mapping is created via a query we assume multiple columns currently
            singleColumn = true;
        }

        if (singleColumn)
        {
            // (Timestamp) implementation
            addDatastoreField(ClassNameConstants.JAVA_SQL_TIMESTAMP);
        }
        else
        {
            // (Timestamp millisecs, Timezone) implementation
            addDatastoreField(ClassNameConstants.LONG); // Timestamp millisecs
            addDatastoreField(ClassNameConstants.JAVA_LANG_STRING); // Timezone
        }
    }

    /*
     * (non-Javadoc)
     * @see org.datanucleus.store.mapping.JavaTypeMapping#getJavaType()
     */
    public Class getJavaType()
    {
        return GregorianCalendar.class;
    }

    /**
     * Accessor for the name of the java-type actually used when mapping the particular datastore field.
     * This java-type must have an entry in the datastore mappings.
     * @param index requested datastore field index.
     * @return the name of java-type for the requested datastore field.
     */
    public String getJavaTypeForDatastoreMapping(int index)
    {
        if (singleColumn)
        {
            // (Timestamp) implementation
            return ClassNameConstants.JAVA_SQL_TIMESTAMP;
        }
        else
        {
            // (Timestamp millisecs, Timezone) implementation
            if (index == 0)
            {
                return ClassNameConstants.LONG;
            }
            else if (index == 1)
            {
                return ClassNameConstants.JAVA_LANG_STRING;
            }
        }
        return null;
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
        if (singleColumn)
        {
            return value;
        }
        else if (index == 0)
        {
            return ((Calendar)value).getTime().getTime();
        }
        else if (index == 1)
        {
            return ((Calendar)value).getTimeZone().getID();
        }
        throw new IndexOutOfBoundsException();
    }

    /*
     * (non-Javadoc)
     * @see org.datanucleus.store.mapping.JavaTypeMapping#setObject(org.datanucleus.ExecutionContext, java.lang.Object,
     *  int[], java.lang.Object)
     */
    public void setObject(ExecutionContext ec, Object preparedStatement, int[] exprIndex, Object value)
    {
        GregorianCalendar cal = (GregorianCalendar) value;
        if (singleColumn)
        {
            // (Timestamp) implementation
            Timestamp ts = null;
            if (cal != null)
            {
                ts = new Timestamp(cal.getTimeInMillis());
            }
            // Server timezone will be applied in the RDBMSMapping at persistence
            getDatastoreMapping(0).setObject(preparedStatement, exprIndex[0], ts);
        }
        else
        {
            // (Timestamp millisecs, Timezone) implementation
            if (cal == null)
            {
                getDatastoreMapping(0).setObject(preparedStatement, exprIndex[0], null);
                getDatastoreMapping(1).setObject(preparedStatement, exprIndex[1], null);
            }
            else
            {
                getDatastoreMapping(0).setLong(preparedStatement, exprIndex[0], cal.getTime().getTime());
                getDatastoreMapping(1).setString(preparedStatement, exprIndex[1], cal.getTimeZone().getID());
            }
        }
    }

    /*
     * (non-Javadoc)
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

        if (singleColumn)
        {
            Timestamp ts = (Timestamp)getDatastoreMapping(0).getObject(resultSet, exprIndex[0]);
            GregorianCalendar cal = new GregorianCalendar();
            cal.setTimeInMillis(ts.getTime());

            String timezoneID = ec.getNucleusContext().getPersistenceConfiguration().getStringProperty("datanucleus.ServerTimeZoneID");
            if (timezoneID != null)
            {
                // Apply server timezone ID since we dont know what it was upon persistence
                cal.setTimeZone(TimeZone.getTimeZone(timezoneID));
            }
            return cal;
        }
        else
        {
            // (Timestamp millisecs, Timezone) implementation
            long millisecs = getDatastoreMapping(0).getLong(resultSet, exprIndex[0]);

            GregorianCalendar cal = new GregorianCalendar();
            cal.setTime(new Date(millisecs));
            String timezoneId = getDatastoreMapping(1).getString(resultSet, exprIndex[1]);
            if (timezoneId != null)
            {
                cal.setTimeZone(TimeZone.getTimeZone(timezoneId));
            }
            return cal;
        }
    }
}