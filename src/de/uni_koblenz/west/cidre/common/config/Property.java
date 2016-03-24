package de.uni_koblenz.west.cidre.common.config;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Fields annotated by this annotation are serialized in the configuration file.
 * The name of this field in the configuration file is defined by
 * {@link #name()}. {@link #description()} contains the description of this
 * property in the configuration file.
 * 
 * @author Daniel Janke &lt;danijankATuni-koblenz.de&gt;
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
@Inherited
public @interface Property {

  public String name();

  public String description();

}
