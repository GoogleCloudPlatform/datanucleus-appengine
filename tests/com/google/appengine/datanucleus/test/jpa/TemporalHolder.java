package com.google.appengine.datanucleus.test.jpa;

import java.util.Date;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

@Entity
public class TemporalHolder
{
    @Id
    Long id;

    @Temporal(TemporalType.DATE)
    Date dateField;

    @Temporal(TemporalType.TIME)
    Date timeField;

    @Temporal(TemporalType.TIMESTAMP)
    Date timestampField;

    public TemporalHolder(long id) {
      this.id = id;
    }

    public void setDateField(Date date) {
        this.dateField = date;
    }
    public void setTimeField(Date date) {
        this.timeField = date;
    }
    public void setTimestampField(Date date) {
        this.timestampField = date;
    }
    public Date getDateField() {
        return dateField;
    }
    public Date getTimeField() {
        return timeField;
    }
    public Date getTimestampField() {
        return timestampField;
    }
}
