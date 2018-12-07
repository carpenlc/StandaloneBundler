package mil.nga.bundler.exceptions;

import java.io.Serializable;

/**
 * Exception thrown by the backing data store interface classes.  This 
 * Exception is thrown if the injection of the 
 * <code>EntityManagerFactory</code> fails. 
 * 
 * @author L. Craig Carpenter
 */
public class EntityManagerFactoryUnavailableException 
		extends Exception 
		implements Serializable {

    /** 
     * Default constructor requiring a message String.
     * @param msg Information identifying why the exception was raised.
     */
    public EntityManagerFactoryUnavailableException(String msg) {
        super(msg);
    }


}
