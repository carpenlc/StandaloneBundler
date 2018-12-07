package mil.nga.bundler.interfaces;

/**
 * Listener interface added in support of the Standalone version of the 
 * bundler.  This interface is invoked when an archive job completes.  
 * This logic replaces the JMS-messaging that occurs within the 
 * Enterprise version of the bundler.
 * 
 * @author L. Craig Carpenter
 */
public interface ArchiveCompletionListenerI {

    /**
     * Single method requiring the archive ID that completed.
     * 
     * @param archiveID the ID of the archive that completed.
     */
	public void notify(long archiveID);
	
}
