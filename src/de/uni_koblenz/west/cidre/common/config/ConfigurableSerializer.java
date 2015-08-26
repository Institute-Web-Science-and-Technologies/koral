package de.uni_koblenz.west.cidre.common.config;

/**
 * For each field marked with the {@link Property} annotation, a method of the
 * following form must be implemented:<br/>
 * <code>public String serialize&lt;nameOfProperty&gt;(V conf)</code><br/>
 * Where <code>&lt;nameOfProperty&gt;</code> is the value of the corresponding
 * {@link Property#name()} field starting with a capital letter and
 * <code>V extends {@link Configurable}</code>.
 */
public interface ConfigurableSerializer {

}
