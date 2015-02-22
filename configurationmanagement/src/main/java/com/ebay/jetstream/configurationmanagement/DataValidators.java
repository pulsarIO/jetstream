/*******************************************************************************
 *  Copyright © 2012-2015 eBay Software Foundation
 *  This program is dual licensed under the MIT and Apache 2.0 licenses.
 *  Please see LICENSE for more information.
 *******************************************************************************/
package com.ebay.jetstream.configurationmanagement;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import javax.xml.stream.FactoryConfigurationError;

import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.InputStreamResource;

import com.ebay.jetstream.config.mongo.ConfigScope;

/**
 * The validators for incoming request;
 * @author weijin
 *
 */
public final class DataValidators {

    public interface DataValidator {
        void validate(String name, Object value)
                throws IllegalArgumentException;
    }

    public static final DataValidator NOT_NULL_OR_EMPTY = new DataValidator() {
        @Override
        public void validate(final String name, final Object value)
                throws IllegalArgumentException {
            String str = (String) value;
            if (str == null || str.length() == 0) {
                throw new IllegalArgumentException(String.format(
                        "%s must neither be null nor empty", name));
            }
        }
    };

    public static final DataValidator NOT_NULL = new DataValidator() {
        @Override
        public void validate(final String name, final Object value)
                throws IllegalArgumentException {
            if (value == null) {
                throw new IllegalArgumentException(String.format(
                        "%s should not be null", name));
            }
        }
    };

    public static final DataValidator IS_VALID_SCOPE = new DataValidator() {
        @Override
        public void validate(final String name, final Object value)
                throws IllegalArgumentException {
            String scope = (String) value;
            if ((!scope.startsWith(ConfigScope.local.name() + ":"))
                    && (!scope.startsWith(ConfigScope.dc.name() + ":"))
                    && (!scope.equals(ConfigScope.global.name()))) {
                throw new IllegalArgumentException(
                        "Unrecogonized scope. The scope must be like: local:server1, serser2 or dc:dc1,dc2 or global");
            }else if(scope.equals(ConfigScope.dc.name() + ":")){
                throw new IllegalArgumentException(
                        "Incomplete scope. Please specify dc name");
            }else if(scope.equals(ConfigScope.local.name() + ":")){
                throw new IllegalArgumentException(
                        "Incomplete scope. Please specify machine name");
            }
        }
    };


    public static final DataValidator IS_VALID_BEAN_DEFINITION = new DataValidator() {
        @Override
        public void validate(final String name, final Object value)
                throws IllegalArgumentException {
            String beanDefinition = (String) value;
            try {
                InputStream input = new ByteArrayInputStream(beanDefinition.getBytes());
                DefaultListableBeanFactory beans = new DefaultListableBeanFactory();
                XmlBeanDefinitionReader reader = new XmlBeanDefinitionReader(beans);
                reader.setValidating(true);
                reader.setValidationMode(XmlBeanDefinitionReader.VALIDATION_XSD);
                reader.loadBeanDefinitions(new InputStreamResource(input));
                validateBeanId(beanDefinition);
            } catch (Exception ex) {
                throw new IllegalArgumentException(String.format(
                        "%s is not valid :%s", name, ex.getMessage()));
            }
        }

        private void validateBeanId(String beanDefinition) throws FactoryConfigurationError, Exception{
            ConfigurationManagementXMLParser xmlParser = ConfigurationManagementXMLParser.getInstance();
            String beanId = ConfigurationManagementXMLParser.getId(beanDefinition);
            if(beanId == null || beanId.isEmpty()) {
                throw new Exception("Bean ID cannot be null or empty");
            }
        }
    };
}