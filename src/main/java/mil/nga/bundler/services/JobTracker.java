package mil.nga.bundler.services;

import java.io.Closeable;
import java.util.List;

import mil.nga.bundler.exceptions.ServiceUnavailableException;
import mil.nga.bundler.interfaces.ArchiveCompletionListenerI;
import mil.nga.bundler.messages.ArchiveMessage;
import mil.nga.bundler.model.ArchiveJob;
import mil.nga.bundler.model.FileEntry;
import mil.nga.bundler.model.Job;
import mil.nga.bundler.services.JobFactoryService.JobFactoryServiceBuilder;
import mil.nga.bundler.types.JobStateType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is based on the logic encapsulated in the 
 * <code>JobTrackerMDB</code> from the Enterprise version of the bundler.
 * This class implements the builder creation pattern to force clients to 
 * supply a valid job ID on instantiation.
 * 
 * This class receives notifications through the 
 * <code>ArchiveCompletionListenerI</code> interface when when an archive job 
 * completes.  It is responsible for ensuring the job state flags and the job
 * statistics information is updated and persisted.
 * 
 * @author L. Craig Carpenter
 */
public class JobTracker implements Closeable, ArchiveCompletionListenerI {
    
    /**
     * Set up the Log4j system for use throughout the class
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(JobTracker.class);
   
    /**
     * Class-level handle to the FileEntryService
     */
    private JobService service;
    
	// Private internal members.
	private final String jobID;
	
    /**
     * Default constructor. 
     */
    public JobTracker(JobTrackerBuilder builder) { 
    	jobID = builder.jobID;
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
     * Calculate the number of archives complete by looping through the 
     * archives and counting how many are complete.
     * 
     * @param job The target job.
     * @return The number of archives complete.
     */
    private int getNumArchivesComplete(Job job) {
        int archivesComplete = 0;
        if (job != null) {
            if ((job.getArchives() != null) && 
                    (job.getArchives().size() > 0)) {
                for (ArchiveJob archive : job.getArchives()) {
                    if (archive.getArchiveState() == JobStateType.COMPLETE) {
                        archivesComplete++;
                    }
                }
            }
            else {
                LOGGER.error("Input Job does not contain any archives.  "
                        + "Unable to calculate the number or archives "
                        + "completed.");
            }
        }
        else {
            LOGGER.error("Input Job ID is null.  Unable to calculate the "
                    + "number of archives completed.");
        }
        return archivesComplete;
    }
    
    /**
     * Calculate the size completed by the archive by looping through the 
     * FileEntry objects and summing the individual size of each archive.
     * 
     * @return The total size of all files in the archive job (uncompressed).
     */
    private long getSizeComplete(List<FileEntry> files) {
        long sizeComplete = 0L;
        if ((files != null) && (files.size() > 0)) {
            for (FileEntry file : files) {
                if (file.getFileState() == JobStateType.COMPLETE) {
                    sizeComplete += file.getSize();
                }
            }
        }
        else {
            LOGGER.error("Input list of files is null or contains zero files.");
        }
        return sizeComplete;
    }
    
    /**
     * Calculate the number of files completed by the archive by looping 
     * through the 
     * FileEntry objects and summing the individual size of each archive.
     * 
     * @return The total size of all files in the archive job (uncompressed).
     */
    private long getFilesComplete(List<FileEntry> files) {
        long numFiles = 0L;
        if ((files != null) && (files.size() > 0)) {
            for (FileEntry file : files) {
                if (file.getFileState() == JobStateType.COMPLETE) {
                    numFiles++;
                }
            }
            if (numFiles != files.size()) {
                LOGGER.warn("There is a mismatch between the number of files "
                        + "in the input list and the number of files that "
                        + "were compressed in the output Archive.  The input "
                        + "list contains [ "
                        + files.size()
                        + " ] files, but [ "
                        + numFiles
                        + " ] were marked complete by the archive processing "
                        + "algorithm.");
            }
        }
        else {
            LOGGER.error("Input list of files is null or contains zero files.");
        }
        return numFiles;
    }
    
    /**
     * We've seen a few rare cases where an archive has completed, but the 
     * database has not been updated prior to the handling the archive 
     * complete message.  One case was a situation where it took over 
     * 25 seconds for the transactions associated with completing the 
     * the archive to commit on one of the nodes.   
     * 
     * @param archive The archive that has complete.  
     */
    private void checkArchive(ArchiveJob archive) { 
        if (archive.getArchiveState() != JobStateType.COMPLETE) {
            LOGGER.warn("Archive complete message received for Job ID [ "
                    + archive.getJobID()
                    + " ], archive ID [ " 
                    + archive.getArchiveID()
                    + " ] but the data store has not been updated.  Updating "
                    + "archive state to ensure that the overall job "
                    + "completes.");
            archive.setArchiveState(JobStateType.COMPLETE);
            archive.setEndTime(System.currentTimeMillis());
        }
    }
    
    /**
     * Update the overall state of the job based on the individual completed
     * archive. 
     * 
     * @param job The Overall Job object.
     * @param archive The individual completed archive file.
     */
    private void updateJobState(Job job, ArchiveJob archive) {
        
        long numFiles              = getFilesComplete(archive.getFiles());
        long totalNumFilesComplete = job.getNumFilesComplete() + numFiles;
        long sizeComplete          = getSizeComplete(archive.getFiles());
        long totalSizeComplete     = job.getTotalSizeComplete() + sizeComplete;
        int  numArchivesComplete   = getNumArchivesComplete(job);
    
        if (totalNumFilesComplete > job.getNumFiles()) {
            LOGGER.warn( "Inconsistency detected in the number of "
                    + "files completed for job ID [ "
                    + job.getJobID()
                    + " ].  Job expects [ "
                    + job.getNumFiles()
                    + " ] files completed, yet calculations based "
                    + "on archives complete show [ "
                    + totalNumFilesComplete
                    + " ].  Updating based on archives.");
            totalNumFilesComplete = job.getNumFiles();
        }
        
        job.setNumFilesComplete(totalNumFilesComplete);
        
        if (totalSizeComplete > job.getTotalSize()) {
            LOGGER.warn("Inconsistency detected in the size of the "
                    + "data completed for job ID [ "
                    + job.getJobID()
                    + " ].  expected size [ "
                    + job.getTotalSize()
                    + " ] size completed, yet calculations based "
                    + "on archives complete indicate [ "
                    + totalSizeComplete
                    + " ].  Updating based on archives.");
            totalSizeComplete = job.getTotalSize();
        }
        
        job.setTotalSizeComplete(totalSizeComplete);
        job.setNumArchivesComplete(numArchivesComplete);
        
        if (job.getNumArchives() == numArchivesComplete) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Marking job ID [ "
                        + job.getJobID() 
                        + " ] complete.");
            }
            job.setState(JobStateType.COMPLETE);
            job.setEndTime(System.currentTimeMillis());
        }
        else { 
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Job ID [ "
                        + job.getJobID() 
                        + " ] not yet complete.  Only [ "
                        + numArchivesComplete
                        + " ] archives complete out of [ "
                        + job.getNumArchives()
                        + " ] total archives.");
            }
        }
    }
    
    /**
     * Getter method for the job ID.
     * 
     * @return value The job ID.
     */
    public String getJobID() {
    	return jobID;
    }
    
    /**
     * Method called when an archive job completes.  This method will 
     * retrieve references to the Job and Archive then call the private 
     * internal methods to update the overall job state.
     * 
     * @param archiveID The archive ID that completed.
     */
    @Override
    public synchronized void notify (long archiveID) {
        
    	try {
                
             LOGGER.info("Archive completed for archive [ "
                     + archiveID
                     + " ].");

             if (getJobService() != null) {
                 
                 Job job = getJobService().getJob(getJobID());
                 
                 if (job != null) {
                     ArchiveJob archive = job.getArchive(archiveID);
                     if (archive != null) {
                         checkArchive(archive);
                         updateJobState(job, archive);
                         getJobService().update(job);
                     }
                     else {
                          LOGGER.error("Unable to retrieve Archive "
                                 + "associated with job ID [ "
                                 + getJobID()
                                 + " ] and archive ID [ "
                                 + archiveID
                                 + " ].");
                     }
                 }
                 else {
                     LOGGER.error("Unable to retrieve Job associated with "
                             + "job ID [ "
                             + getJobID()
                             + " ].");
                 }
             }
         }
		 catch (ServiceUnavailableException sue) {
        	LOGGER.error("Internal system failure.  Target EJB service "
        			+ "is unavailable.  Exception message => [ "
        			+ sue.getMessage()
        			+ " ].");
		 }
    }
    
    /**
     * Internal static class implementing the Builder creation pattern for 
     * new <code>JobTracker</code> objects.  This was added to force clients
     * to supply a valid job ID at object creation time. 
     * 
     * @author L. Craig Carpenter
     */
    public static class JobTrackerBuilder {
    	
    	// Private internal members.
    	private String jobID;
    	
        /**
         * Setter method for the unique job ID.
         * 
         * @param value The unique job ID.
         */
        public JobTrackerBuilder jobID(String value) {
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
        public JobTracker build() {
        	JobTracker message = new JobTracker(this);
            validateJobTrackerObject(message);
            return message;
        }
        
        /**
         * Validate internal member variables.  
         * 
         * @param object The <code>JobFactoryService</code> object to validate.
         * @throws IllegalStateException Thrown if any of the required fields 
         * are not populated.
         */
        private void validateJobTrackerObject(JobTracker object) 
                throws IllegalStateException {
            if ((object.getJobID() == null) || (object.getJobID().isEmpty())) {
            	throw new IllegalStateException("Unable to construct the "
            			+ "JobFactoryService object.  Input Job ID is not "
            			+ "defined.");
            }
        }
    }
}
