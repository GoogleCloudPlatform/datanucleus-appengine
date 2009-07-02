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
package org.datanucleus.store.appengine;

import org.datanucleus.metadata.xml.PersistenceFileMetaDataHandler;
import org.w3c.dom.Attr;
import org.w3c.dom.CDATASection;
import org.w3c.dom.Comment;
import org.w3c.dom.DOMConfiguration;
import org.w3c.dom.DOMException;
import org.w3c.dom.DOMImplementation;
import org.w3c.dom.Document;
import org.w3c.dom.DocumentFragment;
import org.w3c.dom.DocumentType;
import org.w3c.dom.Element;
import org.w3c.dom.EntityReference;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.ProcessingInstruction;
import org.w3c.dom.Text;
import org.w3c.dom.TypeInfo;
import org.w3c.dom.UserDataHandler;
import org.xml.sax.EntityResolver;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.validation.Schema;

/**
 * All of this madness is to work around a bug in DataNucleus 1.1.4 that
 * causes problems when the path of persistence.xml contains spaces in it.
 * The problem is that {@link PersistenceFileMetaDataHandler} reads the
 * path of this file into a {@link URI}, which doesn't support spaces.  To
 * work around this we need to hack deep, repurposing the one piece of static
 * state I could find - the 'dbFactory' field in
 * org.datanucleus.plugin.PluginParser.  This field is typically initialized
 * with the result of {@link DocumentBuilderFactory#newInstance()}.  We use
 * reflection to set the value of this field to our own implementation of
 * {@link DocumentBuilderFactory} that sits at the root of a series of
 * delegates.  We have delegates for:
 * {@link DocumentBuilderFactory}
 * {@link DocumentBuilder}
 * {@link Document}
 * {@link NodeList}
 * {@link Node}
 * {@link Element}
 *
 * The purpose of all these delegates is to ultimately detect when the
 * following element from the plugin.xml in datanucleus-core is parsed:
 * <extension point="org.datanucleus.metadata_handler">
 *    <handler class-name="org.datanucleus.jdo.metadata.JDOMetaDataHandler" name="JDO"
 *        entity-resolver="org.datanucleus.metadata.xml.PluginEntityResolver"/>
 *    <handler class-name="org.datanucleus.metadata.xml.PersistenceFileMetaDataHandler" name="Persistence"
 *        entity-resolver="org.datanucleus.metadata.xml.PluginEntityResolver"/>
 * </extension>
 *
 * We need to detect when this element is parsed so that we can replace
 * {@link PersistenceFileMetaDataHandler} with
 * {@link DatastorePersistenceFileMetaDataHandler} in the config.  This
 * subclass translates spaces to a URI-safe form.
 *
 * TODO(maxr) Get rid of all of this when the DataNucleus bug is fixed. 
 *
 * @author Max Ross <maxr@google.com>
 */
public final class XmlHacks {

  private XmlHacks() {}

  public static void installCustomDocumentBuilderFactory() {
    try {
      Class<?> pluginParserClass = Class.forName("org.datanucleus.plugin.PluginParser");
      java.lang.reflect.Field docBuilderFactoryField = pluginParserClass.getDeclaredField("dbFactory");
      docBuilderFactoryField.setAccessible(true);
      docBuilderFactoryField.set(null, new DatastoreDocumentBuilderFactory());
    } catch (Exception e) {
      throw new RuntimeException("Could not replace doc builder factory", e);
    }
  }

  private static final class DatastoreDocumentBuilderFactory extends DocumentBuilderFactory {

    private DocumentBuilderFactory delegate = DocumentBuilderFactory.newInstance();

    public DocumentBuilder newDocumentBuilder() throws ParserConfigurationException {
      return new DatastoreDocumentBuilder(delegate.newDocumentBuilder());
    }

    public void setAttribute(String s, Object o) throws IllegalArgumentException {
      delegate.setAttribute(s, o);
    }

    public Object getAttribute(String s) throws IllegalArgumentException {
      return delegate.getAttribute(s);
    }

    public void setFeature(String s, boolean b) throws ParserConfigurationException {
      delegate.setFeature(s, b);
    }

    public boolean getFeature(String s) throws ParserConfigurationException {
      return delegate.getFeature(s);
    }
  }

  private static class DatastoreDocumentBuilder extends DocumentBuilder {
    private final DocumentBuilder delegate;

    private DatastoreDocumentBuilder(DocumentBuilder delegate) {
      this.delegate = delegate;
    }

    public void reset() {
      delegate.reset();
    }

    public Document parse(InputStream inputStream) throws SAXException, IOException {
      return new DatastoreDocument(delegate.parse(inputStream));
    }

    public Document parse(InputStream inputStream, String s) throws SAXException, IOException {
      return new DatastoreDocument(delegate.parse(inputStream, s));
    }

    public Document parse(String s) throws SAXException, IOException {
      return new DatastoreDocument(delegate.parse(s));
    }

    public Document parse(File file) throws SAXException, IOException {
      return new DatastoreDocument(delegate.parse(file));
    }

    public Document parse(InputSource inputSource) throws SAXException, IOException {
      return new DatastoreDocument(delegate.parse(inputSource));
    }

    public boolean isNamespaceAware() {
      return delegate.isNamespaceAware();
    }

    public boolean isValidating() {
      return delegate.isValidating();
    }

    public void setEntityResolver(EntityResolver entityResolver) {
      delegate.setEntityResolver(entityResolver);
    }

    public void setErrorHandler(ErrorHandler errorHandler) {
      delegate.setErrorHandler(errorHandler);
    }

    public Document newDocument() {
      return delegate.newDocument();
    }

    public DOMImplementation getDOMImplementation() {
      return delegate.getDOMImplementation();
    }

    public Schema getSchema() {
      return delegate.getSchema();
    }

    public boolean isXIncludeAware() {
      return delegate.isXIncludeAware();
    }
  }

  private static final class DatastoreDocument extends DatastoreNode implements Document {
    private final Document delegate;

    private DatastoreDocument(Document delegate) {
      super(delegate);
      this.delegate = delegate;
    }

    public NodeList getElementsByTagName(String s) {
      return delegate.getElementsByTagName(s);
    }

    public Node importNode(Node node, boolean b) throws DOMException {
      return delegate.importNode(node, b);
    }

    public Element createElementNS(String s, String s1) throws DOMException {
      return delegate.createElementNS(s, s1);
    }

    public Attr createAttributeNS(String s, String s1) throws DOMException {
      return delegate.createAttributeNS(s, s1);
    }

    public NodeList getElementsByTagNameNS(String s, String s1) {
      return delegate.getElementsByTagNameNS(s, s1);
    }

    public Element getElementById(String s) {
      return delegate.getElementById(s);
    }

    public String getInputEncoding() {
      return delegate.getInputEncoding();
    }

    public String getXmlEncoding() {
      return delegate.getXmlEncoding();
    }

    public boolean getXmlStandalone() {
      return delegate.getXmlStandalone();
    }

    public void setXmlStandalone(boolean b) throws DOMException {
      delegate.setXmlStandalone(b);
    }

    public String getXmlVersion() {
      return delegate.getXmlVersion();
    }

    public void setXmlVersion(String s) throws DOMException {
      delegate.setXmlVersion(s);
    }

    public boolean getStrictErrorChecking() {
      return delegate.getStrictErrorChecking();
    }

    public void setStrictErrorChecking(boolean b) {
      delegate.setStrictErrorChecking(b);
    }

    public String getDocumentURI() {
      return delegate.getDocumentURI();
    }

    public void setDocumentURI(String s) {
      delegate.setDocumentURI(s);
    }

    public Node adoptNode(Node node) throws DOMException {
      return delegate.adoptNode(node);
    }

    public DOMConfiguration getDomConfig() {
      return delegate.getDomConfig();
    }

    public void normalizeDocument() {
      delegate.normalizeDocument();
    }

    public Node renameNode(Node node, String s, String s1) throws DOMException {
      return delegate.renameNode(node, s, s1);
    }

    public DocumentType getDoctype() {
      return delegate.getDoctype();
    }

    public DOMImplementation getImplementation() {
      return delegate.getImplementation();
    }

    public Element getDocumentElement() {
      return new DatastoreElement(delegate.getDocumentElement());
    }

    public Element createElement(String s) throws DOMException {
      return delegate.createElement(s);
    }

    public DocumentFragment createDocumentFragment() {
      return delegate.createDocumentFragment();
    }

    public Text createTextNode(String s) {
      return delegate.createTextNode(s);
    }

    public Comment createComment(String s) {
      return delegate.createComment(s);
    }

    public CDATASection createCDATASection(String s) throws DOMException {
      return delegate.createCDATASection(s);
    }

    public ProcessingInstruction createProcessingInstruction(String s, String s1)
        throws DOMException {
      return delegate.createProcessingInstruction(s, s1);
    }

    public Attr createAttribute(String s) throws DOMException {
      return delegate.createAttribute(s);
    }

    public EntityReference createEntityReference(String s) throws DOMException {
      return delegate.createEntityReference(s);
    }
  }

  private static class MixedNodeList implements NodeList {
    private final NodeList delegate;

    private MixedNodeList(NodeList delegate) {
      this.delegate = delegate;
    }

    public Node item(int i) {
      Node result = delegate.item(i);
      if (result instanceof Element) {
        Element ele = (Element) result;
        if (ele.getNodeName().equals("handler") && ele.getAttributes().getLength() == 3) {
          NamedNodeMap attributes = ele.getAttributes();
          Node className = attributes.getNamedItem("class-name");
          Node name = attributes.getNamedItem("name");
          Node entityResolver = attributes.getNamedItem("entity-resolver");
          if (className != null && name != null && entityResolver != null &&
              PersistenceFileMetaDataHandler.class.getName().equals(className.getNodeValue()) &&
              "Persistence".equals(name.getNodeValue()) &&
              "org.datanucleus.metadata.xml.PluginEntityResolver".equals(entityResolver.getNodeValue())) {
            // this is the node
            className.setNodeValue(DatastorePersistenceFileMetaDataHandler.class.getName());
          }
        }
      }
      return result;
    }

    public int getLength() {
      return delegate.getLength();
    }
  }

  private static class DatastoreNode implements Node {
    private final Node delegate;

    DatastoreNode(Node delegate) {
      this.delegate = delegate;
    }

    public String getNodeName() {
      return delegate.getNodeName();
    }

    public String getNodeValue() throws DOMException {
      return delegate.getNodeValue();
    }

    public void setNodeValue(String s) throws DOMException {
      delegate.setNodeValue(s);
    }

    public short getNodeType() {
      return delegate.getNodeType();
    }

    public Node getParentNode() {
      return delegate.getParentNode();
    }

    public NodeList getChildNodes() {
      return new MixedNodeList(delegate.getChildNodes());
    }

    public Node getFirstChild() {
      return delegate.getFirstChild();
    }

    public Node getLastChild() {
      return delegate.getLastChild();
    }

    public Node getPreviousSibling() {
      return delegate.getPreviousSibling();
    }

    public Node getNextSibling() {
      return delegate.getNextSibling();
    }

    public NamedNodeMap getAttributes() {
      return delegate.getAttributes();
    }

    public Document getOwnerDocument() {
      return delegate.getOwnerDocument();
    }

    public Node insertBefore(Node node, Node node1) throws DOMException {
      return delegate.insertBefore(node, node1);
    }

    public Node replaceChild(Node node, Node node1) throws DOMException {
      return delegate.replaceChild(node, node1);
    }

    public Node removeChild(Node node) throws DOMException {
      return delegate.removeChild(node);
    }

    public Node appendChild(Node node) throws DOMException {
      return delegate.appendChild(node);
    }

    public boolean hasChildNodes() {
      return delegate.hasChildNodes();
    }

    public Node cloneNode(boolean b) {
      return delegate.cloneNode(b);
    }

    public void normalize() {
      delegate.normalize();
    }

    public boolean isSupported(String s, String s1) {
      return delegate.isSupported(s, s1);
    }

    public String getNamespaceURI() {
      return delegate.getNamespaceURI();
    }

    public String getPrefix() {
      return delegate.getPrefix();
    }

    public void setPrefix(String s) throws DOMException {
      delegate.setPrefix(s);
    }

    public String getLocalName() {
      return delegate.getLocalName();
    }

    public boolean hasAttributes() {
      return delegate.hasAttributes();
    }

    public String getBaseURI() {
      return delegate.getBaseURI();
    }

    public short compareDocumentPosition(Node node) throws DOMException {
      return delegate.compareDocumentPosition(node);
    }

    public String getTextContent() throws DOMException {
      return delegate.getTextContent();
    }

    public void setTextContent(String s) throws DOMException {
      delegate.setTextContent(s);
    }

    public boolean isSameNode(Node node) {
      return delegate.isSameNode(node);
    }

    public String lookupPrefix(String s) {
      return delegate.lookupPrefix(s);
    }

    public boolean isDefaultNamespace(String s) {
      return delegate.isDefaultNamespace(s);
    }

    public String lookupNamespaceURI(String s) {
      return delegate.lookupNamespaceURI(s);
    }

    public boolean isEqualNode(Node node) {
      return delegate.isEqualNode(node);
    }

    public Object getFeature(String s, String s1) {
      return delegate.getFeature(s, s1);
    }

    public Object setUserData(String s, Object o, UserDataHandler userDataHandler) {
      return delegate.setUserData(s, o, userDataHandler);
    }

    public Object getUserData(String s) {
      return delegate.getUserData(s);
    }
  }

  private static final class DatastoreElementList implements NodeList {
    private final NodeList delegate;

    DatastoreElementList(NodeList delegate) {
      this.delegate = delegate;
    }

    public Element item(int i) {
      Element ele = (Element) delegate.item(i);
      if ("org.datanucleus.metadata_handler".equals(ele.getAttribute("point"))) {
        ele = new DatastoreElement(ele);
      }
      return ele;
    }

    public int getLength() {
      return delegate.getLength();
    }
  }

  private static final class DatastoreElement extends DatastoreNode implements Element {
    private final Element delegate;

    private DatastoreElement(Element delegate) {
      super(delegate);
      this.delegate = delegate;
    }

    public String getTagName() {
      return delegate.getTagName();
    }

    public String getAttribute(String s) {
      return delegate.getAttribute(s);
    }

    public void setAttribute(String s, String s1) throws DOMException {
      delegate.setAttribute(s, s1);
    }

    public void removeAttribute(String s) throws DOMException {
      delegate.removeAttribute(s);
    }

    public Attr getAttributeNode(String s) {
      return delegate.getAttributeNode(s);
    }

    public Attr setAttributeNode(Attr attr) throws DOMException {
      return delegate.setAttributeNode(attr);
    }

    public Attr removeAttributeNode(Attr attr) throws DOMException {
      return delegate.removeAttributeNode(attr);
    }

    public NodeList getElementsByTagName(String s) {
      NodeList list = delegate.getElementsByTagName(s);
      if (s.equals("extension")) {
        list = new DatastoreElementList(list);
      }
      return list;
    }

    public String getAttributeNS(String s, String s1) throws DOMException {
      return delegate.getAttributeNS(s, s1);
    }

    public void setAttributeNS(String s, String s1, String s2) throws DOMException {
      delegate.setAttributeNS(s, s1, s2);
    }

    public void removeAttributeNS(String s, String s1) throws DOMException {
      delegate.removeAttributeNS(s, s1);
    }

    public Attr getAttributeNodeNS(String s, String s1) throws DOMException {
      return delegate.getAttributeNodeNS(s, s1);
    }

    public Attr setAttributeNodeNS(Attr attr) throws DOMException {
      return delegate.setAttributeNodeNS(attr);
    }

    public NodeList getElementsByTagNameNS(String s, String s1) throws DOMException {
      return delegate.getElementsByTagNameNS(s, s1);
    }

    public boolean hasAttribute(String s) {
      return delegate.hasAttribute(s);
    }

    public boolean hasAttributeNS(String s, String s1) throws DOMException {
      return delegate.hasAttributeNS(s, s1);
    }

    public TypeInfo getSchemaTypeInfo() {
      return delegate.getSchemaTypeInfo();
    }

    public void setIdAttribute(String s, boolean b) throws DOMException {
      delegate.setIdAttribute(s, b);
    }

    public void setIdAttributeNS(String s, String s1, boolean b) throws DOMException {
      delegate.setIdAttributeNS(s, s1, b);
    }

    public void setIdAttributeNode(Attr attr, boolean b) throws DOMException {
      delegate.setIdAttributeNode(attr, b);
    }
  }

}
