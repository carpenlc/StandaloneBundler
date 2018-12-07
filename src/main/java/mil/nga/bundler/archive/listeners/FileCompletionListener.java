package mil.nga.bundler.archive.listeners;

import java.io.Closeable;
import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.bundler.exceptions.ServiceUnavailableException;
import mil.nga.bundler.interfaces.FileCompletionListenerI;
import mil.nga.bundler.model.ArchiveElement;
import mil.nga.bundler.services.FileEntryService;
import mil.nga.bundler.types.JobStateType;

/**
 * This class is follows the Observer design pattern.  It is registered 
 * as a listener with classes that extend from 
 * <code>mil.nga.bundler.archive.Archiver</code>.  The <code>Archiver</code>
 * classes invoke the <code>notify()</code> method when a single target 
 * file completes the archive/compression process.  
 * 
 * This logic was added at the request of the MPSU team who wanted better
 * real-time information on the state of a bundle job.  The old algorithm 
 * updated the state of each file in an archive when the entire archive 
 * process was complete.  
 * 
 * Notes: 
 * <li>Because we go back to the data store after each file, the addition 
 * of this logic slows down the bundle process.  If we need performance 
 * improvements, this is a good place to start.</li>
 * <li>This is a non-EJB version of the <code>FileCompletionListener</code> 
 * which is used in the stand-alone bundler deployed to PCF.</li>
 */
public class FileCompletionListener 
		implements Serializable, FileCompletionListenerI, Closeable {

	/**
     * Set up the Log4j system for use throughout the class
     */        
    private static final Logger LOGGER = LoggerFactory.getLogger(
    		FileCompletionListener.class);
    
    /**
     * The job ID that this listener is associated with. 
     */
    private String jobID;
    
    /**
     * The archive ID that this listener is associated with.
     */
    private long archiveID;

    /**
     * Class-level handle to the FileEntryService
     */
    private FileEntryService service;
    
    /**
     * Default constructor. 
     */
    public FileCompletionListener() { }
    
    /**
     * Alternate constructor.
     */
    public FileCompletionListener(String jobID, long archiveID) {
    	setJobID(jobID);
    	setArchiveID(archiveID);
    }
    
    /**
     * Getter method for the class-level <code>FileEntryService</code> object. 
     * @return Handle to the FileEntryService object.
     */
    private FileEntryService getFileEntryService() {
    	if (service == null) {
    		service = new FileEntryService();
    	}
    	return service;
    }
    
    /**
     * Method called when an individual file has completed the bundle 
     * process.  This is used for ensuring that the status of a given 
     * bundle operation is available in real-time.
     * 
     * @param element The file that has just completed the bundle operation. 
     */
    @Override
    public void notify(ArchiveElement element) {
    	if (element != null) {
    		if (LOGGER.isDebugEnabled()) {
    	    	LOGGER.debug("Notify method called for job ID [ "
    	    			+ getJobID() 
    	    			+ " ], archive ID [ "
    	    			+ getArchiveID()
    	    			+ " ].  Element completed => [ "
    	    			+ element.toString()
    	    			+ " ].");
    		}
    		try {
	    		if (getFileEntryService() != null) {
	    			getFileEntryService().updateState(
	    					getJobID(),
	    					getArchiveID(),
	    					element.getURI().toString(),
	    					JobStateType.COMPLETE);
	    		}
    		}
    		catch (ServiceUnavailableException sue) {
            	LOGGER.error("Internal system failure.  Target EJB service "
            			+ "is unavailable.  Exception message => [ "
            			+ sue.getMessage()
            			+ " ].");
    		}
    	}
    }
    
    /**
     * Setter method for the archive ID that this listener is associated with.
     * @return value The archive ID.
     */
    public long getArchiveID() {
    	return archiveID;
    }
    
    /**
     * Getter method for the job ID that this listener is associated with.
     * @return value The job ID.
     */
    public String getJobID() {
    	return jobID;
    }
    
    /**
     * Setter method for the archive ID that this listener is associated with.
     * @param value The archive ID.
     */
    public void setArchiveID(long value) {
    	this.archiveID = value;
    }
    
    /**
     * Setter method for the job ID that this listener is associated with.
     * @param value The job ID.
     */
    public void setJobID(String value) {
    	this.jobID = value;
    }

    /**
     * Method required by the implementation of the <code>Closeable</code> 
     * interface.  This method is responsible for closing the class-level 
     * <code>FileEntryService</code> object. 
     */
    public void close() {
    	if (getFileEntryService() != null) {
    		getFileEntryService().close();
    	}
    }
}
