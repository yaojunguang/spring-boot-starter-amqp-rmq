package com.smarthito.amqp.rmq.config;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.config.InstantiationAwareBeanPostProcessorAdapter;
import org.springframework.util.Assert;

import java.util.function.Supplier;

/**
 * @author yaojunguang
 */
public class AnnotationInstantConfigBeanPostProcessor extends InstantiationAwareBeanPostProcessorAdapter implements BeanFactoryAware {

    protected ConfigurableListableBeanFactory beanFactory;

    protected Object getOrCreateBean(String beanName, Supplier<?> creator) {
        if (beanFactory.containsBean(beanName)) {
            return beanFactory.getBean(beanName);
        } else {
            Object obj = creator.get();
            beanFactory.registerSingleton(beanName, obj);
            return obj;
        }
    }

    @Override
    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
        Assert.isInstanceOf(ConfigurableListableBeanFactory.class, beanFactory,
                this.getClass().getSimpleName() + " requires a ConfigurableListableBeanFactory");
        this.beanFactory = (ConfigurableListableBeanFactory) beanFactory;
    }

}
