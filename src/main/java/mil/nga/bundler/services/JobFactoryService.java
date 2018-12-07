package mil.nga.bundler.services;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import mil.nga.PropertyLoader;
import mil.nga.bundler.ArchiveJobFactory;
import mil.nga.bundler.BundleRequest;
import mil.nga.bundler.FileSystemFactory;
import mil.nga.bundler.FileValidator;
import mil.nga.bundler.UrlGenerator;
import mil.nga.bundler.exceptions.InvalidRequestException;
import mil.nga.bundler.exceptions.PropertiesNotLoadedException;
import mil.nga.bundler.exceptions.ServiceUnavailableException;
import mil.nga.bundler.interfaces.ArchiveCompletionListenerI;
import mil.nga.bundler.interfaces.BundlerConstantsI;
import mil.nga.bundler.messages.BundleRequestMessage;
import mil.nga.bundler.model.Archive;
import mil.nga.bundler.model.ArchiveElement;
import mil.nga.bundler.model.ArchiveJob;
import mil.nga.bundler.model.FileEntry;
import mil.nga.bundler.model.Job;
import mil.nga.bundler.services.BundlerService.BundlerServiceBuilder;
import mil.nga.bundler.types.ArchiveType;
import mil.nga.bundler.types.JobStateType;
import mil.nga.util.FileUtils;
import mil.nga.util.URIUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Session Bean implementation class JobFactoryService
 * 
 * Public methods were modified to be asynchronous.  This was done to more
 * robustly handle the very large bundle requests that we started receiving 
 * from the MPSU team.
 */
public class JobFactoryService 
        extends PropertyLoader 
        implements Runnable, Closeable, BundlerConstantsI {

    /**
     * Set up the Log4j system for use throughout the class
     */        
    private static final Logger LOGGER = LoggerFactory.getLogger(
            JobFactoryService.class);
    
    /**
     * The staging area that will be used for output archives.
     */
    private URI stagingArea;
    
    /**
     * Class-level handle to the JobService
     */
    private JobService service;
    
	// Private internal members.
	private final String               jobID;
	private final BundleRequestMessage bRequestMsg;
	private final BundleRequest        bRequest;
    
    /**
     * Default constructor.
     */
    private JobFactoryService(JobFactoryServiceBuilder builder) { 
        super(PROPERTY_FILE_NAME);
        
        jobID       = builder.jobID;
        bRequest    = builder.bRequest;
        bRequestMsg = builder.bRequestMsg;
        
        // Ensure the S3 file system provider is loaded.
        FileSystemFactory.getInstance().loadS3Filesystem();
        
        String stagingArea = null;
        
        try {
        	stagingArea = super.getProperty(STAGING_DIRECTORY_PROPERTY);
        }
        catch (PropertiesNotLoadedException pnle) {
            LOGGER.error("An unexpected PropertiesNotLoadedException " 
                    + "was encountered.  Please ensure the application "
                    + "is properly configured.  Exception message => [ "
                    + pnle.getMessage()
                    + " ].");
        }
        setStagingArea(stagingArea);
    }
    
    /**
     * Method required by the implementation of the <code>Closeable</code> 
     * interface.  This method is responsible for closing the class-level 
     * <code>FileEntryService</code> object. 
     */
    public void close() {
    	if (getJobService() != null) {
    		getJobService().close();
    	}
    }
    
    /**
     * Getter method for the class-level <code>FileEntryService</code> object. 
     * @return Handle to the FileEntryService object.
     */
    private JobService getJobService() {
    	if (service == null) {
    		service = new JobService();
    	}
    	return service;
    }
    
    /**
     * Generate the name of the accompanying hash file from the calculated
     * name of the output archive.
     * 
     * @param archiveFile The name of the output archive.
     * @return The name of the output hash file.
     */
    private String getHashFile(String archiveFile) {
    	String temp = null;
    	if ((archiveFile != null) && (!archiveFile.isEmpty())) {
	    	temp = FileUtils.removeExtension(archiveFile);
	    	temp = temp + "." + HASH_FILE_EXTENSION;
    	}
    	return temp;
    }
    
    /**
     * Create the output directory associated with the current job ID.
	 *
     * @param jobID The current job ID.
     */
    private void createOutputDirectory(String jobID) {
    	
    	String fullURI = stagingArea.toString();
    	
    	try {
	    	
	    	if (!fullURI.endsWith(File.separator)) {
	    		fullURI = fullURI + File.separator;
	    	}
	    	fullURI = fullURI + jobID;
    	
    		URI newURI = new URI(fullURI);
        	Path p = Paths.get(newURI);
        	if (!Files.exists(p)) {
        		if (LOGGER.isDebugEnabled()) {
        			LOGGER.debug("Creating job output directory [ "
        					+ newURI.toString()
        					+ " ].");
        		}
        		Files.createDirectory(p);
        	}
    	}
    	catch (URISyntaxException use) {
    		// We are creating a URI from another URI, so we should never see 
    		// this exception.
    		LOGGER.warn("Unexpected URISyntaxException raised while "
    				+ "generating the URI associated with the output "
    				+ "staging directory for job ID [ "
    				+ jobID
    				+ " ].  Exception message => [ "
    				+ use.getMessage()
    				+ " ].");
		} catch (IOException ioe) {
    		LOGGER.warn("Unexpected IOException raised while "
    				+ "creating the output directory for job ID [ "
    				+ jobID
    				+ " ].  Target output directory => [ "
    				+ fullURI
    				+ " ].  Exception message => [ "
    				+ ioe.getMessage()
    				+ " ].");
		}
    }
    
    /**
     * Simple method to calculate the target output archive size in
     * bytes.
     * 
     * @param sizeMB Target size measured in MB.
     * @return The target size in bytes.
     */
    private long getSizeInBytes(long sizeMB) {
    	return sizeMB * BYTES_PER_MEGABYTE;
    }
    
    /**
     * Create a concrete instance of a <code>ArchiveJob</code> object that 
     * will be added to the target job.
     * 
     * @param jobID The job ID.
     * @param archive The <code>Archive</code> object to be converted to an
     * <code>ArchiveJob</code> object.
     * @return
     */
    private ArchiveJob createArchiveJobInstance(
    		String  jobID,
    		Archive archive) {
    	
    	int        numFiles   = 0;
    	long       size       = 0;
    	ArchiveJob archiveJob = null;
    	
    	if ((archive.getElementList() != null) && 
    			(archive.getElementList().size() > 0)) {
    		
    		archiveJob = new ArchiveJob(
        			jobID, archive.getID(), archive.getType());
	    	archiveJob.setArchive(
	    			archive.getOutputFile().toString());
	    	archiveJob.setArchiveURL(
	    			UrlGenerator.getInstance().toURL(
	    					archiveJob.getArchive()));
	    	archiveJob.setHash(getHashFile(archiveJob.getArchive()));
	    	archiveJob.setHashURL(
	    			UrlGenerator.getInstance().toURL(
	    					archiveJob.getHash()));
	    	archiveJob.setArchiveState(JobStateType.NOT_STARTED);
	    	
    		for (ArchiveElement element : archive.getElementList()) {
    			numFiles++;
    			size += element.getSize();
	    		archiveJob.add(
	    				createFileEntryInstance(
	    						jobID, 
	    						archive.getID(), 
	    						element));
	    	}
    		
    		archiveJob.setNumFiles(numFiles);
    		archiveJob.setSize(size);
    		
    	}
    	else {
    		LOGGER.error("The input Archive object contains no associated files "
    				+ "to archive.  A null ArchiveJob will be returned.");
    	}
    	return archiveJob;
    }
    
    /**
     * Create a concrete instance of a <code>FileEntry</code> object that 
     * we can add to the target Job.
     * 
     * @param jobID The job ID.
     * @param archiveID The archive ID.
     * @param element The ArchiveElement object.
     * @return A constructed and populated <code>FileEntry</code> object.
     */
    private FileEntry createFileEntryInstance(
    		String jobID, 
    		long archiveID,
    		ArchiveElement element) {
    	return new FileEntry(
    			jobID, 
    			archiveID, 
    			element.getURI().toString(), 
    			element.getEntryPath(), 
    			element.getSize());
    }
    
    /**
     * Method used to construct a job object that will be used to notify the 
     * caller of an invalid request.
     * 
     * @param jobID The job ID.
     * @param userName The client user submitting the job.
     * @param type The type of output archive to create.
     * @param archiveSize The target size of the output archive.
     * @return A failed Job.
     */
    private Job createBogusJobInstance(
    		String        jobID, 
    		String        userName, 
    		ArchiveType   type,
    		long          archiveSize) {
    	Job job = new Job();
    	job.setJobID(jobID);
    	job.setUserName(userName);
    	job.setArchiveType(type);
    	job.setArchiveSize(getSizeInBytes(archiveSize));
    	job.setState(JobStateType.INVALID_REQUEST);
    	return job;
    }
    
    /**
     * Create a concrete instance of a <code>Job</code> object that will 
     * be used to 
     * 
     * @param jobID The job ID.
     * @param userName The client user submitting the job.
     * @param type The type of output archive to create.
     * @param archiveSize The target size of the output archive.
     * @return A constructed and populated Job object.
     */
    private Job createJobInstance(
    		String        jobID, 
    		String        userName, 
    		ArchiveType   type,
    		long         archiveSize,
    		List<Archive> archives) {
    	
    	int  numFiles    = 0;
    	int  numArchives = 0;
    	long size        = 0;
    	Job  job         = new Job();
    	
    	job.setJobID(jobID);
    	job.setUserName(userName);
    	job.setArchiveType(type);
    	job.setArchiveSize(getSizeInBytes(archiveSize));
    	
    	if ((archives != null) && (archives.size() > 0)) {
    		for (Archive archive : archives) {
    			ArchiveJob aJob = createArchiveJobInstance(jobID, archive);
    			if (aJob != null) {
    				numArchives++;
    				numFiles += aJob.getNumFiles();
    				size     += aJob.getSize();
    				job.addArchive(aJob);
    			}
    			else {
    				LOGGER.warn("Null ArchiveJob object received.  "
    						+ "The ArchiveJob will not be added to the "
    						+ "target job.");
    			}
        	}
    	}
    	else {
    		LOGGER.error("There are no archives in the target job.  Setting "
    				+ "job state to INVALID_REQUEST.");
    		job.setState(JobStateType.INVALID_REQUEST);
    	}
    	
    	job.setTotalSize(size);
    	job.setNumFiles(numFiles);
    	job.setNumArchives(numArchives);
    	
    	return job;
    }

    public void createJob(String jobID, BundleRequestMessage request) throws ServiceUnavailableException {
    	
    	long startTime = System.currentTimeMillis();
    	Job  job       = null;
    	
    	try {
    		
	    	// Validate and expand the input file list.
	        List<FileEntry> files = FileValidator
	                .getInstance()
	                .validate(request.getFiles());
	    
	        if ((files != null) && (!files.isEmpty())) {
	        	
	        	if (LOGGER.isDebugEnabled()) {
		        	LOGGER.debug("Input request resulted in [ "
		        			+ files.size()
		        			+ " ] validated files to bundle.");
		        }
	        	
		        ArchiveJobFactory factory = new ArchiveJobFactory(
		        		request.getType(),
		        		request.getMaxSize(),
		        		jobID,
		        		request.getOutputFilename());
		        
		        List<Archive> archives = factory
		        		.createArchivesFromFileEntry(files);
		        
		        if ((archives != null) && (archives.size() > 0)) {
			        job = createJobInstance(
			        		jobID, 
			        		request.getUserName(), 
			        		request.getType(), 
			        		request.getMaxSize(),
			        		archives);
		        }
		        else {
		        	LOGGER.error("There are no archive jobs to process.  "
		        			+ "Setting job state to INVALID_REQUEST.");	
		        	job = createBogusJobInstance(
			        		jobID, 
			        		request.getUserName(), 
			        		request.getType(), 
			        		request.getMaxSize());
		        }
	        }
	        else {
	        	LOGGER.error("Validation algorithm revealed no files to "
	        			+ "bundle.  Setting job state to INVALID_REQUEST.");
	        	job = createBogusJobInstance(
		        		jobID, 
		        		request.getUserName(), 
		        		request.getType(), 
		        		request.getMaxSize());
	        }
        
    	}
    	catch (InvalidRequestException ire) {
    		LOGGER.error("InvalidRequestException raised while validating "
    				+ "the input job.  Exception message => [ "
    				+ ire.getMessage()
    				+ " ].");
    		job = createBogusJobInstance(
	        		jobID, 
	        		request.getUserName(), 
	        		request.getType(), 
	        		request.getMaxSize());
    	}
    	
    	// Save the job to the target data store.
    	if (getJobService() != null) {
    		getJobService().persist(job);
    	}
    	else {
    		LOGGER.error("Unable to look up the target JobService EJB.  "
    				+ "Unable to persist job.  Unable to process incoming "
    				+ "request.");
    	}
    	
    	if (LOGGER.isDebugEnabled()) {
    		LOGGER.debug("Job ID [ "
    				+ jobID 
    				+ " created in [ "
    				+ (System.currentTimeMillis() - startTime)
    				+ " ].");
    	}
    	
        runJob(job);
    }
    
    public void createJob(String jobID, BundleRequest request) 
    		throws ServiceUnavailableException {
    	
    	long startTime = System.currentTimeMillis();
    	Job  job       = null;
    	
    	try {
    		
	    	// Validate and expand the input file list.
	        List<FileEntry> files = FileValidator
	                .getInstance()
	                .validateStringList(request.getFiles());
	        
	        if ((files != null) && (!files.isEmpty())) {
	        
	        	if (LOGGER.isDebugEnabled()) {
		        	LOGGER.debug("Input request resulted in [ "
		        			+ files.size()
		        			+ " ] validated files to bundle.");
		        }
	        	
		        ArchiveJobFactory factory = new ArchiveJobFactory(
		        		request.getType(),
		        		request.getMaxSize(),
		        		jobID,
		        		request.getOutputFilename());
		        
		        List<Archive> archives = factory
		        		.createArchivesFromFileEntry(files);
		        
		        if ((archives != null) && (archives.size() > 0)) {
			        job = createJobInstance(
			        		jobID, 
			        		request.getUserName(), 
			        		request.getType(), 
			        		request.getMaxSize(),
			        		archives);
		        }
		        else {
		        	LOGGER.error("There are no archive jobs to process.  "
		        			+ "Setting job state to INVALID_REQUEST.");	
		        	job = createBogusJobInstance(
			        		jobID, 
			        		request.getUserName(), 
			        		request.getType(), 
			        		request.getMaxSize());
		        }
	        }
	        else {
	        	LOGGER.error("Validation algorithm revealed no files to "
	        			+ "bundle.  Setting job state to INVALID_REQUEST.");
	        	job = createBogusJobInstance(
		        		jobID, 
		        		request.getUserName(), 
		        		request.getType(), 
		        		request.getMaxSize());
	        }
	    }
		catch (InvalidRequestException ire) {
			LOGGER.error("InvalidRequestException raised while validating "
					+ "the input job.  Exception message => [ "
					+ ire.getMessage()
					+ " ].");
			job = createBogusJobInstance(
	        		jobID, 
	        		request.getUserName(), 
	        		request.getType(), 
	        		request.getMaxSize());
		}
    	
    	// Save the job to the target data store.
    	if (getJobService() != null) {
    		getJobService().persist(job);
    	}
    	else {
    		LOGGER.error("Unable to look up the target JobService EJB.  "
    				+ "Unable to persist job.  Unable to process incoming "
    				+ "request.");
    	}
    	
    	if (LOGGER.isDebugEnabled()) {
    		LOGGER.debug("Job ID [ "
    				+ jobID 
    				+ " created in [ "
    				+ (System.currentTimeMillis() - startTime)
    				+ " ].");
    	}
    	
    	runJob(job);
    }
    
    /**
     * Getter method for the client supplied BundleRequest object.
     * 
     * @return The client supplied BundleRequest object.
     */
    public BundleRequest getBundleRequest() {
    	return bRequest;
    }
    
    /**
     * Getter method for the client supplied BundleRequestMessage object.
     * 
     * @return The client supplied BundleRequestMessage object.
     */
    public BundleRequestMessage getBundleRequestMessage() {
    	return bRequestMsg;
    }
    
    /**
     * Getter method for the job ID.
     * @return value The job ID.
     */
    public String getJobID() {
    	return jobID;
    }
    
    /**
     * Method containing the threaded content to execute.  This method is 
     * overridden from the <code>Runnable</code> interface.
     */
    public void run() {
    	try {
	    	if (getBundleRequestMessage() != null) {
	    		createJob(getJobID(), getBundleRequestMessage());
	    	}
	    	else {
	    		createJob(getJobID(), getBundleRequest());
	    	}
    	}
    	catch (ServiceUnavailableException sue) {
    		
    	}
    }

    /**
     * Public method used to start the thread encapsulating the processing 
     * for a user submitted job.
     */
    public void start() {
    	LOGGER.info("Starting thread to create job ID [ "
    			+ getJobID()
    			+ " ]...");
    	new Thread(this).start();
    }
    
    /**
     * Start execution of the target job.
     * 
     * @param job The job to execute.
     */
    public void runJob(Job job) {
    	
    	if (job != null) {
    		
    		// Ensure the output directory is created.
			createOutputDirectory(job.getJobID());
    		
			if (job.getState() == JobStateType.NOT_STARTED) {
    			
				// Construct the listener object to receive notifications when 
				// individual archive jobs complete.  This replaces the JMS 
				// logic contained in the Enterprise bundler.
				ArchiveCompletionListenerI listener = 
						new JobTracker.JobTrackerBuilder()
							.jobID(job.getJobID())
							.build();
				
				for (ArchiveJob archive : job.getArchives()) {
					
					LOGGER.info("Starting bundle service for job ID [ "
							+ archive.getJobID()
							+ " ] and archive ID [ "
							+ archive.getArchiveID()
							+ " ].");
					new BundlerService.BundlerServiceBuilder()
							.jobID(archive.getJobID())
							.archiveID(archive.getArchiveID())
							.completionListener(listener)
							.build()
							.start();
				}
				
				try {
					job.setState(JobStateType.IN_PROGRESS);
					job.setStartTime(System.currentTimeMillis());
					getJobService().update(job);
				}
				catch (ServiceUnavailableException sue) {
					LOGGER.warn("Unexpected ServiceUnavailableException "
							+ "raised while updating job status for job ID [ "
							+ job.getJobID()
							+ " ].  Exception message => [ "
							+ sue.getMessage()
							+ " ].");
				}
    		}
    		else {
    			LOGGER.warn("Attempted to start a job with a state of [ "
    					+ job.getState().getText()
    					+ " ].  Job will not be started.");
    		}
    	}
    	else {
    		LOGGER.warn("Input job is null.  Nothing to do.");
    	}
    }
    
    /**
     * Setter method for the output staging area.
     * @param value
     */
    private void setStagingArea(String value) {
    	if ((value == null) || (value.isEmpty())) {
    		value = System.getProperty("java.io.tmpdir");
    	}
    	stagingArea = URIUtils.getInstance().getURI(value);
    }
    
    /**
     * Internal static class implementing the Builder creation pattern for 
     * new <code>JobFactoryService</code> objects.  
     * 
     * @author L. Craig Carpenter
     */
    public static class JobFactoryServiceBuilder implements BundlerConstantsI {
    	
    	// Private internal members.
    	private String               jobID;
    	private BundleRequestMessage bRequestMsg;
    	private BundleRequest        bRequest;
    	
        /**
         * Setter method for the client supplied BundleRequest object.
         * 
         * @param value The client supplied BundleRequest object.
         */
        public JobFactoryServiceBuilder bundleRequest(BundleRequest value) {
        	bRequest = value;
            return this;
        }
        
        /**
         * Setter method for the client supplied BundleRequestMessage object.
         * 
         * @param value The client supplied BundleRequestMessage object.
         */
        public JobFactoryServiceBuilder bundleRequestMessage(BundleRequestMessage value) {
        	bRequestMsg = value;
            return this;
        }
        
        /**
         * Setter method for the unique job ID.
         * 
         * @param value The unique job ID.
         */
        public JobFactoryServiceBuilder jobID(String value) {
        	jobID = value;
            return this;
        }
        
        /**
         * Construct a new <code>JobFactoryService</code> object.  
         * 
         * @return A constructed and validated <code>JobFactoryService</code> 
         * object.
         * @throws IllegalStateException Thrown if any of the input data is 
         * out of range.  
         */
        public JobFactoryService build() {
        	JobFactoryService message = new JobFactoryService(this);
            validateJobFactoryServiceObject(message);
            return message;
        }
        
        /**
         * Validate internal member variables.  
         * 
         * @param object The <code>JobFactoryService</code> object to validate.
         * @throws IllegalStateException Thrown if any of the required fields 
         * are not populated.
         */
        private void validateJobFactoryServiceObject(JobFactoryService object) 
                throws IllegalStateException {
            if ((object.getJobID() == null) || (object.getJobID().isEmpty())) {
            	throw new IllegalStateException("Unable to construct the "
            			+ "JobFactoryService object.  Input Job ID is not "
            			+ "defined.");
            }

            if ((bRequest == null) && (bRequestMsg == null)) {
            	throw new IllegalStateException("Unable to construct the "
            			+ "JobFactoryService object.  No request available "
            			+ "to process.");
            }
            
        }
    }
}
