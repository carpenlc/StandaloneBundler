package mil.nga.bundler.services;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.bundler.archive.ArchiveFactory;
import mil.nga.bundler.archive.listeners.FileCompletionListener;
import mil.nga.bundler.exceptions.ArchiveException;
import mil.nga.bundler.exceptions.ServiceUnavailableException;
import mil.nga.bundler.exceptions.UnknownArchiveTypeException;
import mil.nga.bundler.interfaces.ArchiveCompletionListenerI;
import mil.nga.bundler.interfaces.BundlerConstantsI;
import mil.nga.bundler.interfaces.BundlerI;
import mil.nga.bundler.interfaces.FileCompletionListenerI;
import mil.nga.bundler.messages.ArchiveMessage;
import mil.nga.bundler.model.ArchiveElement;
import mil.nga.bundler.model.ArchiveJob;
import mil.nga.bundler.model.FileEntry;
import mil.nga.bundler.types.JobStateType;
import mil.nga.util.FileUtils;
import mil.nga.util.URIUtils;

/**
 * This class will execute the bundle operation on a given Archive job defined
 * by the job ID and archive ID.  This non-EJB version implements the 
 * <code>Runnable</code> interface and performs the bundle operation within 
 * a Thread. 
 * 
 * @author L. Craig Carpenter
 */
public class BundlerService implements Runnable, Closeable, BundlerConstantsI {

	/**
     * Set up the Log4j system for use throughout the class
     */        
    private static final Logger LOGGER = LoggerFactory.getLogger(
    		BundlerService.class);
    
    /**
     * List of listeners that have registered to be notified when individual 
     * archives have completed processing.
     */
    private List<ArchiveCompletionListenerI> listeners;
    
    /**
     * Used for thread-safety.
     */
    private Object MUTEX = new Object();
    
    // Private internal members
    private final String            jobID;
    private final long             archiveID;
    private final long             startTime;
    private final ArchiveJobService service;
    
	/**
     * Default constructor ensuring the required internal member objects are
     * set.
     */
    private BundlerService(BundlerServiceBuilder builder) { 
    	archiveID = builder.archiveID;
    	jobID     = builder.jobID;
    	service   = builder.service;
    	startTime = System.currentTimeMillis();
    	addListener(builder.listener);
    }
    
    /**
     * Add a listener for archive completion.  This listener is used for 
     * updating the handling overall job status.
     * 
     * @param listener Listener to be notified when an archive operation 
     * completes.
     */
    private void addListener(ArchiveCompletionListenerI listener) {
    	if (listener != null) {
    		if (listeners == null) {
    			listeners = new ArrayList<ArchiveCompletionListenerI>();
    		}
    		synchronized (MUTEX) {
    			if (!listeners.contains(listener)) {
    				listeners.add(listener);
    			}
    		}
    	}
    }
    
    /**
     * This method is part of the implementation of the Observer design 
     * pattern. This allows users of classes extending from Archiver to 
     * be notified when processing associated with a given file are 
     * complete.
     * 
     * @param value The <code>ArchiveElement</code> object that has changed
     * it's internal state.
     */
    private void notify(long value) {
    	if ((listeners != null) && (listeners.size() > 0)) {
    		List<ArchiveCompletionListenerI> localListeners = null;
    		synchronized(MUTEX) {
    			localListeners = new ArrayList<ArchiveCompletionListenerI>(listeners);
    		}
    		for (ArchiveCompletionListenerI listener : localListeners) {
    			listener.notify(value);
    		}
     	}
    	else {
    		LOGGER.info("Archive job for job ID => [ "
    				+ getJobID()
    				+ " ] and archive ID [ "
    				+ value
    				+ " ] complete.");
    	}
    }
    
    /**
     * Method required by the implementation of the <code>Closeable</code> 
     * interface.  This method is responsible for closing the class-level 
     * <code>ArchiveJobService</code> object. 
     */
    @Override
    public void close() {
    	if (getArchiveJobService() != null) {
    		getArchiveJobService().close();
    	}
    }
    
    /**
     * Method used to update the archive to reflect that archive processing 
     * has started.
     * 
     * @throws ServiceUnavailableException Thrown if we are unable to 
     * establish a connection to the back-end data store.
     */
    private ArchiveJob startArchiveJob() throws ServiceUnavailableException {
    	
    	ArchiveJob archiveJob = getArchiveJobService().getArchiveJob(
				getJobID(), 
				getArchiveID());
            
        if (archiveJob != null) {
        	archiveJob.setHostName(FileUtils.getHostName());
        	archiveJob.setServerName(DEFAULT_SERVER_NAME);
        	archiveJob.setStartTime(System.currentTimeMillis());
        	archiveJob.setArchiveState(JobStateType.IN_PROGRESS);      
        	getArchiveJobService().update(archiveJob);
        }
	    else {
	        LOGGER.error("Unable to find archive to process for "
	                    + "job ID [ "
	                    + getJobID()
	                    + " ] and archive ID [ "
	                    + getArchiveID()
	                    + " ].");
	    }
        return archiveJob;
    }
    
    /**
     * Method used to update the archive to reflect that archive processing 
     * has ended.
     * 
     * @param endState The final end state of the archive job.
     * @throws ServiceUnavailableException Thrown if we are unable to 
     * establish a connection to the back-end data store.
     */
    private void endArchiveJob(JobStateType endState) {
    	
    	long endTime = System.currentTimeMillis();
    	
    	try {
	    	if (LOGGER.isDebugEnabled()) {
	            if (LOGGER.isDebugEnabled()) {
	                LOGGER.debug("Archive processing for job ID [ "
	                        + getJobID()
	                        + " ] and archive ID [ "
	                        + getArchiveID()
	                        + " ].  Completed in [ "
	                        + (endTime - startTime)
	                        + " ] ms.");
	            }
	    	}
	    	
	    	ArchiveJob archiveJob = getArchiveJobService().getArchiveJob(
					getJobID(), 
					getArchiveID());
	            
	        if (archiveJob != null) {
	        	archiveJob.setArchiveState(endState); 
	        	if (endState == JobStateType.COMPLETE) {
	        		getArchiveFileSize(
	                		archiveJob.getArchive());
	        	}
	        	archiveJob.setEndTime(endTime);
	        	getArchiveJobService().update(archiveJob);
	        }
		    else {
		        LOGGER.error("Unable to find archive to process for "
		                    + "job ID [ "
		                    + getJobID()
		                    + " ] and archive ID [ "
		                    + getArchiveID()
		                    + " ].");
		    }
    	}
    	catch (ServiceUnavailableException sue) {
    		LOGGER.error("ServiceUnavailableException raised while attempting "
    				+ "to set the completion status of archive job with "
    				+ "job ID [ "
    				+ getJobID()
    				+ " ] and archive ID [ "
    				+ getArchiveID() 
    				+ " ].  Exception message => [ "
    				+ sue.getMessage()
    				+ " ].");
    	}
    	notify(getArchiveID());
    }
    
    /**
     * Map the input list of <code>FileEntry</code> objects to an output list of 
     * <code>ArchiveElement</code> objects to pass into the bundler algorithm.
     *  
     * @param files A list of <code>FileEntry</code> objects to bundle.
     * @return a list containing <code>ArchiveElement</code> objects.  The 
     * output may be empty, but it will not be null.
     */
    public static List<ArchiveElement> getArchiveElements(List<FileEntry> files) {
    	List<ArchiveElement> elements = new ArrayList<ArchiveElement>();
    	if ((files != null) && (files.size() > 0)) {
    		for (FileEntry file : files) {
    			elements.add(new ArchiveElement.ArchiveElementBuilder()
    								.size(file.getSize())
    								.entryPath(file.getEntryPath())
    								.uri(URIUtils.getInstance()
    										.getURI(file.getFilePath()))
    							.build());
    		}
    	}
    	else {
    		LOGGER.warn("Input list of FileEntry objects is null or empty.  "
    				+ "Output list will also be empty.");
    	}
    	return elements;
    }
    


    /**
     * 
     */
    @Override
    public void run() {
        
        try (FileCompletionListener listener = new FileCompletionListener(
        			getJobID(), getArchiveID())) {
        	
        	// Retrieve the ArchiveJob from the data store.
        	ArchiveJob archive = startArchiveJob();
            
            if (archive != null) {
            	
                // Get the concrete Bundler object.
                BundlerI bundler = ArchiveFactory.getInstance()
                					.getBundler(archive.getArchiveType());
              
                // Set up the listener for the completion of individual file 
                // archives.  This was added at the request of the MPSU team and 
                // may need to be removed if too much of an impact to 
                // performance.
                bundler.addFileCompletionListener(listener);
                
                // Here's where the magic happens.
                bundler.bundle(
                		getArchiveElements(archive.getFiles()), 
                		URIUtils.getInstance().getURI(archive.getArchive()));
                
                // Generate the hash for the completed archive.
                new HashGeneratorService.HashGeneratorServiceBuilder()
                		.inputFile(archive.getArchive())
                		.outputFile(archive.getHash())
                	.build()
                	.generate();
                
                // Update the status of the job appropriately.
                endArchiveJob(JobStateType.COMPLETE);
                
            }
            else {
                LOGGER.error("Unable to find an ARCHIVE_JOBS record matching "
                		+ "job ID [ "
                		+ getJobID()
                		+ " ] and archive ID [ "
                		+ getArchiveID()
                		+ " ].");
            }
        }
        // This exception can never be raised.  
        catch (UnknownArchiveTypeException uate) {}
        catch (IOException ioe) {
            LOGGER.error("Unexpected IOException raised while "
                    + "creating the output archive.  Archive "
                    + "state will be set to ERROR for job ID [ "
                    + getJobID()
                    + " ] archive ID [ "
                    + getArchiveID()
                    + " ].  Error message [ "
                    + ioe.getMessage()
                    + " ].");
            endArchiveJob(JobStateType.ERROR);
        }
        catch (ArchiveException ae) {
            LOGGER.error("Unexpected ArchiveException raised "
                    + "while "
                    + "creating the output archive.  Archive "
                    + "state will be set to ERROR for job ID [ "
                    + getJobID()
                    + " ] archive ID [ "
                    + getArchiveID()
                    + " ].  Error message [ "
                    + ae.getMessage()
                    + " ].");
            endArchiveJob(JobStateType.ERROR);
        }
        catch (ServiceUnavailableException sue) {
        	LOGGER.error("Internal system failure.  JPA service "
        			+ "is unavailable.  Exception message => [ "
        			+ sue.getMessage()
        			+ " ].");
        }
        finally {
        	getArchiveJobService().close();
        }
    }
    
    /**
     * Getter method for the service responsible for updating the ArchiveJob 
     * in the back-end data store. 
     * 
     * @return value The ArchiveJobService object.
     */
    public ArchiveJobService getArchiveJobService() {
    	return service;
    }
    
    /**
     * Getter method for the archive ID that this listener is associated with.
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
     * Simple method used to retrieve the size of the created archive file.
     * 
     * @param archive The completed Archive object.
     */
    private long getArchiveFileSize(String archive) {
        
        long size = 0L;
        
        if ((archive != null) && (!archive.isEmpty())) {
        	URI output = URIUtils.getInstance().getURI(archive);
        	
            Path p = Paths.get(output);
            if (Files.exists(p)) {
            	try {
            		size = Files.size(p);
            	}
            	catch (IOException ioe) {
            		LOGGER.error("Unexpected IOException while attempting "
            				+ "to obtain the size associated with file [ "
            				+ output.toString()
            				+ " ].  Exception message => [ "
            				+ ioe.getMessage()
            				+ " ].");
            	}
            }
            else {
                LOGGER.error("The expected output archive file [ "
                        + archive
                        + " ] does not exist.");
            }
        }
        else {
        	LOGGER.error("The identified output archive file is null or "
        			+ "empty.  The final output archive size will not be "
        			+ "set.");
        }
        return size;
    }
    
    /**
     * Public method used to start the thread encapsulating the processing 
     * for a single archive job.
     */
    public void start() {
    	LOGGER.info("Starting thread to process archive job for job ID [ "
    			+ getJobID()
    			+ " ] and archive ID [ "
    			+ getArchiveID()
    			+ " ]...");
    	new Thread(this).start();
    }
    
    /**
     * Internal static class implementing the Builder creation pattern for 
     * new BundleService objects.  
     * 
     * @author L. Craig Carpenter
     */
    public static class BundlerServiceBuilder implements BundlerConstantsI {
        
    	// Private internal members
        private String                     jobID     = null;
        private long                      archiveID = -1L;
        private ArchiveJobService          service   = null;
        private ArchiveCompletionListenerI listener = null;
        
        /**
         * Setter method for the unique job ID.
         * @param value The unique job ID.
         */
        public BundlerServiceBuilder jobID(String value) {
        	jobID = value;
            return this;
        }
        
        /**
         * Setter method for the ID number associated with the archive.
         * @param value The ID number identifying the target archive.
         */
        public BundlerServiceBuilder archiveID(long value) {
            archiveID = value;
            return this;
        }
        
        /**
         * Setter method for the archive completion listener.  
         * @param value The archive completion listener.
         */
        public BundlerServiceBuilder completionListener(
        		ArchiveCompletionListenerI value) {
        	listener = value;
            return this;
        }
        
        /**
         * Setter method for the ID number associated with the archive.
         * @param value The ID number identifying the target archive.
         */
        public BundlerServiceBuilder archiveMessage(ArchiveMessage value) {
            if (value != null) {
	        	archiveID = value.getArchiveID();
	            jobID     = value.getJobID();
            }
            else {
            	throw new IllegalStateException("ArchiveMessage is null.");
            }
            return this;
        }
        
        /**
         * Construct a new <code>BundlerService</code> object.
         * @return A constructed and validated <code>BundlerService</code> 
         * object.
         * @throws IllegalStateException Thrown if any of the input data is 
         * out of range.  
         */
        public BundlerService build() {
        	service = new ArchiveJobService();
        	BundlerService message = new BundlerService(this);
            validateBundlerServiceObject(message);
            return message;
        }
        
        /**
         * Validate internal member variables.  
         * 
         * @param object The <code>BundlerService</code> object to validate.
         * @throws IllegalStateException Thrown if any of the required fields 
         * are not populated.
         */
        private void validateBundlerServiceObject(BundlerService object) 
                throws IllegalStateException {
            if (object.getArchiveJobService() == null) {
            	throw new IllegalStateException("Unable to construct the "
            			+ "ArchiveJobService object.");
            }
            if ((object.getJobID() == null) || 
                    (object.getJobID().isEmpty())) {
                throw new IllegalStateException("Job ID not populated.");
            }
            if ((object.getArchiveID() < 0) ||  
                    (object.getArchiveID() > MAX_NUM_ARCHIVES)) {
                throw new IllegalStateException("Invalid archive ID received [ "
                        + object.getArchiveID() 
                        + " ].  Archive IDs must be between [ 0 ] and [ "
                        + MAX_NUM_ARCHIVES
                        + " ].");
            }
        }
    }
}
