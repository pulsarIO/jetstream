/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
/**
 *
 */
package com.ebay.jetstream.management;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.jmx.export.annotation.ManagedResource;

import com.ebay.jetstream.util.CommonUtils;

/**
 * 
 * 
 */
public class Management {
  public static class BeanFolder extends ConcurrentHashMap<String, Object> {
    private static final long serialVersionUID = 1L;

    protected BeanFolder() {
    }
  }

  private static Map<String, Class<? extends ManagedResourceFormatter>> s_resourceFormatters = new HashMap<String, Class<? extends ManagedResourceFormatter>>();
  private static List<ManagementListener> s_listeners = new ArrayList<ManagementListener>();
  private static final String PATH_SEPARATOR = "/";
  private static final String PATH_RELATIVE = "./";
  private static BeanFolder s_directory = new BeanFolder();

  /**
   * Adds a managed bean to the directory. The full path of the bean is taken from the ManagedResource annotation for
   * the bean.
   * 
   * @param bean
   *            the bean to manage.
   * @return the path to the bean.
   */
  public static String addBean(Object bean) {
    return addBean(null, bean);
  }

  /**
   * Adds a managed bean to the directory. Missing path nodes are created as necessary. An IllegalArgumentException is
   * thrown if the path is illegal. The path given is combined with any path from a ManagedResource annotation. If the
   * annotation has an objectName prefixed with "./", the annotated path is added to the end of the given path, else it
   * is added to the beginning of the given path.
   * 
   * @param path
   *            the path to the managed bean, separated by '/' characters, with the bean name at the path end, or null.
   *            If null, the path is taken completely from the ManagedResource annotation for the bean.
   * 
   * @param bean
   *            the bean to manage.
   * 
   * @return the path to the bean.
   * 
   * @throws IllegalArgumentException
   *             if the path already exists or includes a non-leaf managed bean.
   */
  public static String addBean(String path, Object bean) {
    String rpath = getResourcePath(path, bean);
    String[] pc = rpath.split(PATH_SEPARATOR);
    Object object = getPath(pc, 1);
    if (!(object instanceof BeanFolder))
      throw new IllegalArgumentException("bean already exists at " + rpath);
    ((BeanFolder) object).put(pc[pc.length - 1], bean);
    notifyListeners(bean, rpath, ManagementListener.BeanAddedEvent.class);
    return rpath;
  }

  /**
   * Gets a managed bean or bean folder. An exception is thrown if the path is illegal or does not exist.
   * 
   * @param path
   *            the path to the Bean or BeanFolder.
   * 
   * @return the Bean or BeanFolder described by the given path.
   */
  public static Object getBeanOrFolder(String path) {
    return CommonUtils.isEmptyTrimmed(path) ? s_directory : getPath(path.split(PATH_SEPARATOR), 0);
  }

  public static List<ManagementListener> getManagementListeners() {
    return Collections.unmodifiableList(s_listeners);
  }

  /**
   * Gets a new instance of a ManagedResourceFormatter for the given format.
   * 
   * @param format
   *            the string format name.
   * 
   * @return the instance of ManagedResourceFormatter for the given format.
   * 
   * @throws InstantiationException
   * @throws IllegalAccessException
   */
  public static ManagedResourceFormatter getResourceFormatter(String format) throws InstantiationException,
      IllegalAccessException {
    Class<? extends ManagedResourceFormatter> clazz = s_resourceFormatters.get(format);
    return clazz == null ? null : clazz.newInstance();
  }

  public static Set<String> getResourceFormatters() {
    return s_resourceFormatters.keySet();
  }

  public static void registerManagementListener(ManagementListener listener) {
    s_listeners.add(listener);
  }

  /**
   * Registers a ManagedResourceFormatter to a format name.
   * 
   * @param format
   *            the string format name to register for.
   * @param formatterClass
   *            the resource formatter class that implements ManagedResourceFormatter. The class must define a default
   *            (no parameters) constructor.
   */
  public static void registerResourceFormatter(String format, Class<? extends ManagedResourceFormatter> formatterClass) {
    s_resourceFormatters.put(format, formatterClass);
  }

  /**
   * Removes a bean or tree from the directory.
   * 
   * @param path
   *            the path to a bean or BeanFolder
   * @return true iff anything was removed
   */
  public static boolean removeBeanOrFolder(String path) {
    return removePath(path.split(PATH_SEPARATOR), s_directory, 0) > 0;
  }

  /**
   * Removes a bean or tree from the directory.
   * 
   * @param path
   *            the path to a bean or folder, or null. If null, the full path to the bean is calculated from the
   *            ManagedResource annotation for the bean.
   * @param bean
   *            the bean to remove
   * @return true iff anything was removed
   */
  public static boolean removeBeanOrFolder(String path, Object bean) {
    return removeBeanOrFolder(getResourcePath(path, bean));
  }

  private static String checkLevel(String pathLevel) {
    if (pathLevel == null || pathLevel.length() == 0)
      throw new IllegalArgumentException("empty path level found");
    return pathLevel;
  }

  /**
   * Gets the path to BeanFolder or managed bean, and optionally adds missing path components to the directory.
   * 
   * @param path
   *            the path to the managed bean, including the managed bean name.
   * @param mode
   *            N <= 0: no add and limit traversal to Nth from end; N > 0: add to the folder to Nth from end.
   * @return the target of the path if found, or the containing folder (mode = 1).
   * @throws IllegalArgumentException
   *             if the path has a managed bean that is not at the path end, or if part of the path is missing (mode ==
   *             -1).
   */
  private static Object getPath(String[] path, int mode) {
    BeanFolder folder = s_directory;
    Object object = null;
    // traverse the path
    int istart = 0;
    for (int limit = path.length - (mode > 0 ? 0 : mode); istart < limit; istart++) {
      object = folder.get(checkLevel(path[istart]));
      if (object instanceof BeanFolder)
        // this level is a folder, go to next (or end)
        folder = (BeanFolder) object;
      else if (object == null) {
        if (mode <= 0)
          // missing a level
          throw new IllegalArgumentException("no beans exist at " + path[istart]);
        else {
          // mode > 0, create missing levels
          for (object = folder, limit = path.length - mode; istart < limit; istart++) {
            folder = (BeanFolder) object;
            folder.put(checkLevel(path[istart]), object = new BeanFolder());
          }
          break;
        }
      }
      // object not a folder, and not null
      else if (istart != path.length - 1)
        // found a bean in the middle of the path
        throw new IllegalArgumentException("expecting BeanFolder, found managed bean at " + path[istart]);
    }
    return object;
  }

  private static String getResourcePath(String path, Object bean) {
    String rpath = path;
    ManagedResource mr = bean.getClass().getAnnotation(ManagedResource.class);
    if (mr != null) {
      String xpath = mr.objectName();
      if (xpath == null || xpath.length() == 0)
        xpath = mr.value();
      if (xpath != null && xpath.length() > 0) {
        rpath = path == null ? "" : xpath.startsWith(PATH_RELATIVE) ? path + PATH_SEPARATOR : PATH_SEPARATOR + path;
        rpath = xpath.startsWith(PATH_RELATIVE) ? rpath + xpath.substring(PATH_RELATIVE.length()) : xpath + rpath;
      }
    }
    return rpath;
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="REC_CATCH_EXCEPTION", justification="Want to suprress exceptions here.")
  private static void notifyListeners(Object bean, String path,
      Class<? extends ManagementListener.BeanChangedEvent> eventClass) {
    if (!s_listeners.isEmpty()) {
      ManagementListener.BeanChangedEvent event = null;
      try {
        event = eventClass.getConstructor(Object.class, String.class).newInstance(bean, path);
        for (ManagementListener listener : s_listeners)
          try {
            listener.beanChanged(event);
          }
          catch (Exception e) {
          }
      }
      catch (Exception e) {
      }
    }
  }

  private static int removePath(String[] path, BeanFolder folder, int index) {
    String s = checkLevel(path[index]);
    Object object = folder.get(s);
    int result = 0;
    // recurse to sub-folders
    if (object instanceof BeanFolder && index < path.length - 1) {
      result = removePath(path, (BeanFolder) object, index + 1);
    }
    // remove path leaf and folders above that becomes empty due to this
    if (object != null && (result == path.length - index && folder.size() == 1 || index == path.length - 1)) {
      folder.remove(s);
      if (result++ == 0)
        notifyListeners(object, s, ManagementListener.BeanRemovedEvent.class);
    }
    return result;
  }

  private Management() {
  }
}
