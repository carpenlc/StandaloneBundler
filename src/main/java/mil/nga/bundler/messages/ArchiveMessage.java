package mil.nga.bundler.messages;

import java.io.Serializable;

import mil.nga.bundler.interfaces.BundlerConstantsI;

/**
 * Simple Java Bean class used to pass information associated with individual 
 * archive jobs between nodes in the cluster.  The object will contain enough
 * information to uniquely identify an ArchiveJob to process.
 * 
 * @author L. Craig Carpenter
 */
public class ArchiveMessage implements Serializable {

    /**
     * Eclipse-generated serialVersionUID
     */
    private static final long serialVersionUID = -1900130496098489193L;
    
    // Private internal members
    private final String jobID;
    private final long   archiveID;
    
    /**
     * Default constructor.
     */
    private ArchiveMessage(ArchiveMessageBuilder builder) {
        jobID     = builder.jobID;
        archiveID = builder.archiveID;
    }
    
    /**
     * Getter method for the Archive ID field.
     * 
     * @return The ID associated with the target archive.
     */
    public long getArchiveID() {
        return archiveID;
    }
    
    /**
     * Getter method for the Job ID field.
     * 
     * @return The job ID associated with the target archive.
     */
    public String getJobID() {
        return jobID;
    }
    
    /**
     * Overridden toString method.
     * @return String version of the ArchiveMessage object.
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ArchiveMessage: job ID => [ ");
        sb.append(getJobID());
        sb.append(" ], archive ID => [ ");
        sb.append(getArchiveID());
        sb.append(" ].");
        return sb.toString();
    }
    
    /**
     * Internal static class implementing the Builder creation pattern for 
     * new BundleRequestBuilder objects.  
     * 
     * @author L. Craig Carpenter
     */
    public static class ArchiveMessageBuilder implements BundlerConstantsI {
        
        // Private internal members
        private String jobID     = null;
        private long   archiveID = -1L;
        
        /**
         * Construct a new BundleRequest object.  
         * 
         * @return A constructed and validated BundleRequest object.
         * @throws IllegalStateException Thrown if any of the input data is 
         * out of range.  
         */
        public ArchiveMessage build() {
            ArchiveMessage message = new ArchiveMessage(this);
            validateArchiveMessageObject(message);
            return message;
        }
        
        /**
         * Setter method for the unique job ID.
         * 
         * @param value The unique job ID.
         */
        public ArchiveMessageBuilder jobID(String value) {
            jobID = value;
            return this;
        }
        
        /**
         * Setter method for the ID number associated with the archive.
         * 
         * @param value The ID number identifying the target archive.
         */
        public ArchiveMessageBuilder archiveID(long value) {
            archiveID = value;
            return this;
        }
        
        /**
         * Validate internal member variables.  
         * 
         * @param object The <code>ArchiveMessage</code> object to validate.
         * @throws IllegalStateException Thrown if any of the required fields 
         * are not populated.
         */
        private void validateArchiveMessageObject(ArchiveMessage message) 
                throws IllegalStateException {
            
            if ((message.getJobID() == null) || 
                    (message.getJobID().isEmpty())) {
                throw new IllegalStateException("Job ID not populated.");
            }
            if ((message.getArchiveID() < 0) ||  
                    (message.getArchiveID() > MAX_NUM_ARCHIVES)) {
                throw new IllegalStateException("Invalid archive ID received [ "
                        + message.getArchiveID() 
                        + " ].  Archive IDs must be between [ 0 ] and [ "
                        + MAX_NUM_ARCHIVES
                        + " ].");
            }
        }
    }
}
