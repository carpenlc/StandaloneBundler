package mil.nga.bundler.exceptions;

import java.io.Serializable;

/**
 * Exception thrown by the backing data store interface classes.  This 
 * Exception is thrown if we are unable to construct an 
 * <code>EntityManager</code> from the injected 
 * <code>EntityManagerFactory</code> object.
 * 
 * @author L. Craig Carpenter
 */
public class EntityManagerUnavailableException 
		extends Exception 
		implements Serializable {

	/** 
     * Default constructor requiring a message String.
     * @param msg Information identifying why the exception was raised.
     */
    public EntityManagerUnavailableException(String msg) {
        super(msg);
    }
}
