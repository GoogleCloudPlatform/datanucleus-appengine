/**********************************************************************
Copyright (c) 2009 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
**********************************************************************/
package com.google.appengine.datanucleus.test.jdo;

import com.google.appengine.api.datastore.Key;
import com.google.appengine.datanucleus.Utils;


import java.util.ArrayList;
import java.util.List;

import javax.jdo.annotations.Element;
import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

/**
 * @author Max Ross <maxr@google.com>
 */
public class UnownedJoinsJDO {

  @PersistenceCapable(detachable = "true")
  public static class Student {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Long id;

    private int grade;

    @Element(dependent = "true")
    private List<Key> courses = new ArrayList<Key>();

    @Persistent
    private Key major;

    public int getGrade() {
      return grade;
    }

    public void setGrade(int grade) {
      this.grade = grade;
    }

    public List<Key> getCourses() {
      return courses;
    }

    public void setCourses(List<Key> courses) {
      this.courses = courses;
    }

    public Key getMajor() {
      return major;
    }

    public void setMajor(Key major) {
      this.major = major;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Student student = (Student) o;

      if (id != null ? !id.equals(student.id) : student.id != null) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return id != null ? id.hashCode() : 0;
    }
  }
  
  @PersistenceCapable(detachable = "true")
  public static class Course {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Key key;

    private String department;

    public Key getKey() {
      return key;
    }

    public String getDepartment() {
      return department;
    }

    public void setDepartment(String department) {
      this.department = department;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Course course = (Course) o;

      if (key != null ? !key.equals(course.key) : course.key != null) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return key != null ? key.hashCode() : 0;
    }
  }

  @PersistenceCapable(detachable = "true")
  public static class Major {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Key key;

    private String school;

    public Key getKey() {
      return key;
    }

    public void setKey(Key key) {
      this.key = key;
    }

    public String getSchool() {
      return school;
    }

    public void setSchool(String school) {
      this.school = school;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Major major = (Major) o;

      if (key != null ? !key.equals(major.key) : major.key != null) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      int result = key != null ? key.hashCode() : 0;
      result = 31 * result + (school != null ? school.hashCode() : 0);
      return result;
    }
  }

  public static Student newStudent(int grade, Course... courses) {
    return newStudent(grade, null, courses);
  }

  public static Student newStudent(int grade, Major major, Course... courses) {
    Student s = new Student();
    s.setGrade(grade);
    List<Key> courseKeys = Utils.newArrayList();
    for (Course c : courses) {
      courseKeys.add(c.getKey());
    }
    s.setCourses(courseKeys);
    if (major != null) {
      s.setMajor(major.getKey());
    }
    return s;
  }

  public static Course newCourse(String dept) {
    Course c = new Course();
    c.setDepartment(dept);
    return c;
  }

  public static Major newMajor(String school) {
    Major m = new Major();
    m.setSchool(school);
    return m;
  }
}
