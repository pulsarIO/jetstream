/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.servlet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;

import com.ebay.jetstream.util.CommonUtils;

/**
 * Contains the properties required for hosting a ssl port/connection.
 * 
 * @author varavindan
 * 
 */
public class SSLServerHost implements InitializingBean {

  private int maxIdleTime = 10000;

  private String keyStorePath;
  private String keyStorePassword;

  /**
   * Injectable from Spring but if not present would be set to the keyStore *
   */
  private String trustStorePath;
  private String trustStorePassword;

  private final Logger LOGGER = LoggerFactory.getLogger("com.ebay.jetstream.servlet.SSLServerHost");

  public void afterPropertiesSet() throws Exception {
    if (CommonUtils.isEmptyTrimmed(getKeyStorePassword()) && CommonUtils.isEmptyTrimmed(getKeyStorePath()))
      throw new Exception("Keystore Path/Password required for hosting a SSL port !!!");

    LOGGER.warn( "Keystore Path And Password Not Empty/Nul. Keystore Path: " + getKeyStorePath());
        
    if (CommonUtils.isEmptyTrimmed(getTrustStorePath()))
      setTrustStorePath(getKeyStorePath());

    if (CommonUtils.isEmptyTrimmed(getTrustStorePassword()))
      setTrustStorePassword(getKeyStorePassword());
  }

  /**
   * @return the keyStorePassword
   */
  public String getKeyStorePassword() {
    return keyStorePassword;
  }

  /**
   * @return the keyStorePath
   */
  public String getKeyStorePath() {
    return keyStorePath;
  }

  /**
   * @return the maxIdleTime
   */
  public int getMaxIdleTime() {
    return maxIdleTime;
  }

  /**
   * @return the trustStorePassword
   */
  public String getTrustStorePassword() {
    return trustStorePassword;
  }

  /**
   * @return the trustStorePath
   */
  public String getTrustStorePath() {
    return trustStorePath;
  }

  /**
   * @param keyStorePassword
   *          the keyStorePassword to set
   */
  public void setKeyStorePassword(String keyStorePassword) {
    this.keyStorePassword = keyStorePassword;
  }

  /**
   * @param keyStorePath
   *          the keyStorePath to set
   */
  public void setKeyStorePath(String keyStorePath) {
    this.keyStorePath = keyStorePath;
  }

  /**
   * @param maxIdleTime
   *          the maxIdleTime to set
   */
  public void setMaxIdleTime(int maxIdleTime) {
    this.maxIdleTime = maxIdleTime;
  }

  /**
   * @param trustStorePassword
   *          the trustStorePassword to set
   */
  public void setTrustStorePassword(String trustStorePassword) {
    this.trustStorePassword = trustStorePassword;
  }

  /**
   * @param trustStorePath
   *          the trustStorePath to set
   */
  public void setTrustStorePath(String trustStorePath) {
    this.trustStorePath = trustStorePath;
  }

}
