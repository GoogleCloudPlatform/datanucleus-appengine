#!/usr/bin/python2.4
import os
import sys
import getopt
import xml.dom.minidom

# Starts parsing the XML file containing the book data entries
#

default_local = "http://localhost:8080/library"
default_corp = "http://gptestshop.prom.corp.google.com/library"
default_uri = default_corp
default_files = ("book.xml", "tech.xml")

class initPromLibrary:
  """Utility to initialize the Prometheus Library

  We repeatedly use wget to send Add actions to the 
  Prometheus Library with lastname, firstname, year 
  and title parameters in order to build up a catalaog 
  of entries to run the automated Selenium tests. 
  The catalog name in the xml file is used to specify 
  the PromLibrary entityname parameter.

  The catalog is built up from a set of xml files
  currently consisting of book.xml and tech.xml.
  The latter is a small catalog that illustrates all of
  the fields.

  <?xml version="1.0" encoding="utf-8"?>
  <catalog>
  <name>TechBooks</name>
  <book>
  <title>Python in a Nutshell</title>
  <author>Alex Martelli</author>
  <year>2003</year>
  </book>
  <book>
  <title>HTML and XHTML</title>
  <author>Bill Kennedy</author>
  <year>2007</year>
  </book>
  </catalog>

  Each book entry consists of:

  title - the title of the book
  author - the author of the book which gets divided into
           a lastname and firstname
  year -  the year of the book

  The uri is specified with the -u option on the command line.
  The default uri is http://gptestshop.prom.corp.google.com/library
  which is the version on the Prometheus corp cluster.

  This is also the uri that is used if -u corp is passed.
  If -u local is used, the the uri is http://localhost:8080/library
  or the locally deployed version.
  """

  def handleCatalog(self, catalog, uri):
    """Parse the XML file and get the catalog name
    and call handleBooks to deal with the list of books
    """
    XmlBooks = catalog.getElementsByTagName("book")
    Xmlname =  catalog.getElementsByTagName("name")[0]
    name = Xmlname.childNodes[0].data
    self.handleBooks(XmlBooks, name, uri)

  def handleBooks(self, XmlBooks , name, uri):
    """For each book in the list call handleBook 
    to extract the entry info.
    """
    for XmlBook in XmlBooks:
      self.handleBook(XmlBook, name, uri)

  def handleBook(self, XmlBook, entityname, uri):
    """Extracts the book data entries from the XML file and 
    calls AddBook to send it to the Prometheus Library
    """
    Xmltitle = XmlBook.getElementsByTagName("title")[0]
    XmlAuthor = XmlBook.getElementsByTagName("author")[0]
    Xmldate = XmlBook.getElementsByTagName("year")[0]
    title =  Xmltitle.childNodes[0].data
    year  =  Xmldate.childNodes[0].data
    fullname = XmlAuthor.childNodes[0].data.split(None,2)
    firstname = fullname[0]
    lastname  = fullname[1]
    self.AddBook(uri, lastname, firstname, title, year, entityname)

  def AddBook(self, uri, lastname, firstname, title, year, entityname):
    """Add a Book to the Prometheus library by building the
    parameter query string and calling wget. The results
    from wget are redirected to /dev/null. This is used
    rather than --spider because these types of requests
    are not honored by Prometheus.
    """
    querystring = '?'
    for arg in [ "lastname=%s" % lastname, "firstname=%s" % firstname, 
            "title=%s" % title, "year=%s" % year, "action_type=Add",
            "entity=%s" % entityname ]:
      querystring = "%s&%s" % (querystring, arg)

    postdatastring = '--post-data "%s"' % (querystring)
    cmd = 'wget'
    opt = "-O /dev/null"
    cmd_str = "%s %s %s %s" % (cmd, opt, postdatastring, uri)
    print cmd_str
    os.system(cmd_str)
   

def main(argv):
  """Perform initialization, parse command line options and
  arguments, and output usage messages. 
  If multiple filenames are provided, loop through
  each one and call the handleCatalog to process the XML.
  """
  uri = default_uri
  cmdline_params = argv[1:]

  try:  
    optlist, args = getopt.getopt(cmdline_params, 'u:', ['uri='])
  except getopt.GetoptError:
    print 'Usage: initPromLibrary.py [-u uri] [files]'
    print '    Default files = %s %s' % (default_files[0], default_files[1])
    print '    Default uri = %s' % (default_uri)
    print "    Shorthand uri's"
    print '    -u corp =  %s' % (default_corp)
    print '    -u local = %s' % (default_local)
    sys.exit(2)

  if optlist:
    uri = optlist[0][1]
    if uri == 'local':
      uri = default_local
    if uri == 'corp':
      uri = default_corp

  if not args: 
    args = default_files

  for arg in args:
    if arg:
      print arg
      if arg[0] == '/':
        xmlfilename = arg
      else:
        xmlfilename = os.path.join(os.path.dirname(__file__), arg)
        dom = xml.dom.minidom.parse(xmlfilename)
        cat = initPromLibrary() 
        cat.handleCatalog(dom, uri)

if __name__ == '__main__':
  main(sys.argv)


