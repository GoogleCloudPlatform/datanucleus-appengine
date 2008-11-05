// Copyright 2008 Google Inc. All Rights Reserved.
package com.google.appengine.library;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * @author kjin@google.com (Kevin Jin)
 */
public class SetDataServiceName extends HttpServlet {

  @Override
  public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    String newName = req.getParameter("DataServiceName");
    if (newName != null) {
      BookDataServiceFactory.setServiceName(newName);
    }

    printForm(resp);
  }

  private void printForm(HttpServletResponse resp) throws IOException {
    PrintWriter out = resp.getWriter();
    out.println("<form action=\"/setDataServiceName\" method=\"get\">");
    out.println("<select name=\"DataServiceName\">");
    String currentName = BookDataServiceFactory.getServiceName();
    for (String name : BookDataServiceFactory.dataServiceNameToClassMap.keySet()) {
      out.print("<option");
      if (currentName.equals(name)) {
        out.print(" selected=\"selected\"");
      }
      out.println(" value=\"" + name + "\">" + name + "</option>");
    }
    out.println("</select>");
    out.println("<input type=\"submit\" value=\"Set Data Service API\">");
    out.println("</form>");
  }
}
