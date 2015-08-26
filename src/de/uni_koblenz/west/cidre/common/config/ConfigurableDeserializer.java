package de.uni_koblenz.west.cidre.common.config;

/**
 * For each field marked with the {@link Property} annotation, a method of the
 * following form must be implemented:<br/>
 * <code>public void deserialize&lt;nameOfProperty&gt;(V conf, String value)</code>
 * <br/>
 * Where <code>&lt;nameOfProperty&gt;</code> is the value of the corresponding
 * {@link Property#name()} field starting with a capital letter.
 */
public interface ConfigurableDeserializer<V extends Configurable> {

}
