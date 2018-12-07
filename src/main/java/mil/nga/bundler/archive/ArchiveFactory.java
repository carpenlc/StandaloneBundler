package mil.nga.bundler.archive;

import mil.nga.bundler.interfaces.BundlerI;
import mil.nga.bundler.types.ArchiveType;
//import mil.nga.bundler.archive.BZip2Archiver;
//import mil.nga.bundler.archive.GZipArchiver;
import mil.nga.bundler.archive.TarArchiver;
//import mil.nga.bundler.archive.ZipArchiver;
import mil.nga.bundler.exceptions.UnknownArchiveTypeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory class designed to instantiate concrete implementations of 
 * objects that can be utilized to create output archive files.  This 
 * class is implemented using the singleton design pattern and will 
 * support output archives of types contained in the ArchiveType 
 * enumeration. 
 * 
 * @author L. Craig Carpenter
 */
public class ArchiveFactory {

    /**
     * Set up the Log4j system for use throughout the class
     */        
    final static Logger LOGGER = LoggerFactory.getLogger(
            ArchiveFactory.class);
    
    /**
     * Hidden constructor enforcing the Singleton design pattern.
     */
    private ArchiveFactory() {}

    /**
     * Accessor method for the Singleton instance of the ArchiveFactory.
     * object.
     * 
     * @return The Singleton instance.
     */
    public static ArchiveFactory getInstance() {
        return ArchiveFactoryHolder.getSingleton();
    }
    
    /**
     * Construct a concrete instance of a class that will be able to 
     * construct the output archive requested.
     * 
     * @param type The type of archiver requested.
     * @return A concrete class implementing the logic required for 
     * constructing an output archive file.
     */
    public BundlerI getBundler(ArchiveType type) 
    		throws UnknownArchiveTypeException {
        
        switch (type) {
        	case ZIP:
        		if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Client requested ZIP archive format.");
                }
        		return new ZipArchiver();
        	case AR:
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Client requested AR archive format.");
                }
                return new ArArchiver();
        	case CPIO:
	            if (LOGGER.isDebugEnabled()) {
	                LOGGER.debug("Client requested CPIO archive format.");
	            }
	            return new CpioArchiver();
        	case TAR:
	            if (LOGGER.isDebugEnabled()) {
	                LOGGER.debug("Client requested TAR archive format.");
	            }
	            return new TarArchiver();
        	case GZIP:
        		if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Client requested GZIP archive format.");
                }
                return new GZipArchiver();
        	case BZIP2:
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Client requested BZIP2 archive format.");
                }
                return new BZip2Archiver();
            default:
            	throw new UnknownArchiveTypeException("Client requested an "
            			+ "unsupported archive type [ "
            			+ type.getText()
            			+ " ].");
        }
    }
    
    /**
     * Static inner class used to construct the Singleton object.  This class
     * exploits the fact that classes are not loaded until they are referenced
     * therefore enforcing thread safety without the performance hit imposed
     * by the <code>synchronized</code> keyword.
     * 
     * @author L. Craig Carpenter
     */
    public static class ArchiveFactoryHolder {
        
        /**
             * Reference to the Singleton instance of the ArchiveFactory
         */
        private static ArchiveFactory _instance = new ArchiveFactory();
    
        /**
         * Accessor method for the singleton instance of the ArchiveFactory.
         * @return The Singleton instance of the ArchiveFactory.
         */
        public static ArchiveFactory getSingleton() {
            return _instance;
        }
        
    }
}
