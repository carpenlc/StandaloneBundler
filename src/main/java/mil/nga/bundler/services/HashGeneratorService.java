package mil.nga.bundler.services;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import mil.nga.bundler.types.HashType;
import mil.nga.util.URIUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the non-EJB version of the HashGeneratorService.  This class is 
 * invoked after the archive files are created.  It will construct a hash of 
 * the supplied input file.  
 * 
 * Note: We switched to using the commons codec classes because we found 
 * issues when converting the output hashes to Base64 using the JDK classes 
 * (specifically, leading 0s were being dropped).
 * 
 * @author L. Craig Carpenter
 */
public class HashGeneratorService {

	/**
     * Set up the Log4j system for use throughout the class
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(
    				HashGeneratorService.class);
    
    // Private internal members
    private final URI      inputFile;
    private final URI      outputFile;
    private final HashType hashType;
    
	/**
     * Default constructor ensuring the required internal member objects are
     * set.
     */
    private HashGeneratorService(HashGeneratorServiceBuilder builder) { 
    	hashType   = builder.hashType;
    	inputFile  = builder.inputFile;
    	outputFile = builder.outputFile;
    }
    
    /**
     * Construct the hexadecimal-based hash of the input file.  If the input
     * file doesn't exist, or errors are encountered during hash generation the 
     * returned hash is null.
     * 
     * @param inputFile String containing the full path to a file on which
     * the requested hash should be applied.
     * @param hashType The type of hash to create 
     * @see <code>mil.nga.bundler.types.HashType</code>
     * @return The hash value as a hex string.
     */
    public String getHash() {
        
        String hash = null;

        Path p = Paths.get(getInputFile());
        
        if (p != null) { 
            if (Files.exists(p)) {
                
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Generating [ "
                            + getHashType().getText()
                            + " ] hash for file [ "
                            + p.toString()
                            + " ].");
                }
                
                long startTime = System.currentTimeMillis();
                switch (getHashType()) {
                    case MD5 : 
                        hash = getMD5Hash();
                        break;
                    case SHA1:
                        hash = getSHA1Hash();
                        break;
                    case SHA256:
                        hash = getSHA256Hash();
                        break;
                    case SHA384:
                        hash = getSHA384Hash();                        
                        break;
                    case SHA512:
                        hash = getSHA512Hash();                        
                        break;
                }
                
                long elapsedTime = System.currentTimeMillis() - startTime;
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(
                           "Hash type [ "
                            + hashType.getText()
                            + " ] for file [ "
                            + p.toString()
                            + " ] created in [ "
                            + Long.toString(elapsedTime)
                            + " ] ms.");
                }
            }
            else {
                LOGGER.error("Input file does not exists.  Input file "
                        + "specified [ "
                        + p.toString()
                        + " ].");
            }
        }
        else {
            LOGGER.error("The require input Path parameter is null or empty. "
                    + " The output hash file will not be generated.");
        }
        return hash;
    }
    
    
    /**
     * Generate a hash associated with the input file and store it in the output
     * file.
     */
    public void generate() {
        
        long startTime = System.currentTimeMillis();

        if (getInputFile() != null) {
            if (getOutputFile() != null) {
                
            	if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Creating hash for file [ "
                            + getInputFile().toString() 
                            + " ].");
                }
            	
                Path p = Paths.get(getInputFile());
                if (Files.exists(p)) {
                    String hash = getHash();
                    saveHash(hash);
                	if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Hash for file [ "
                                + getInputFile().toString() 
                                + " ] created in [ "
                                + (System.currentTimeMillis() - startTime)
                                + " ] ms.");
                    }
                }
                else {
                    LOGGER.error("Input file does not exists.  Input file "
                            + "specified [ "
                            + inputFile
                            + " ].");
                }
            }
            else {
                    LOGGER.error("The require input file parameter is null or empty. "
                            + " The output hash file will not be generated.");
            }
        }
        else {
            LOGGER.error("The require input file parameter is null or empty.  "
                    + "The hash will not be generated.");
        }
    }
    
    /**
     * Getter method for the type of hash to generate.
     * @return The type of hash to generate.
     */
    public HashType getHashType() {
    	return hashType;
    }
    
    /**
     * Getter method for the Path object to the input file.
     * @return The Path object associated with the input file.
     */
    public URI getInputFile() {
    	return inputFile;
    }
    
    /**
     * Getter method for the Path object to the output file.
     * @return The Path object associated with the output file.
     */
    public URI getOutputFile() {
    	return outputFile;
    }
    
    /**
     * Calculate the MD5 hash using the Apache Commons Codec classes.  
     * 
     * @param file The file we need the hash for.
     * @return The calculated MD5 hash.
     */
    public String getMD5Hash() {

        String          hash = null;

        if (getInputFile() != null) {
	        try (InputStream is = Files.newInputStream(Paths.get(getInputFile()))) {
	            hash = org.apache.commons.codec.digest.DigestUtils.md5Hex(is);
	        }
	        catch (IOException ioe) {
	            LOGGER.error(
	                 "Unexpected IOException encountered while generating "
	                 + "the [ " 
	                 + HashType.MD5.getText() 
	                 + " ] hash for file [ "
	                 + getInputFile().toString()
	                 + " ].  Exception message => [ "
	                 + ioe.getMessage()
	                 + " ].  Method will return a null hash.");
	        }
        }
	    else {
	    	LOGGER.error("Input file Path is null.  Hash will not be generated.");
	    }
        return hash;
    }
    
    /**
     * Calculate the SHA-1 hash using the Apache Commons Codec classes.
     * Note: SHA-1 hash generation seems to take about twice as long as MD5 
     * hash generation.
     * 
     * @param file The file we need the hash for.
     * @return The calculated SHA1 hash.
     */
    public String getSHA1Hash() {

        String          hash = null;
        
        if (getInputFile() != null) {
	        try (InputStream is = Files.newInputStream(
	        			Paths.get(getInputFile()))) {
	            hash = org.apache.commons.codec.digest.DigestUtils.sha1Hex(is);
	        }
	        catch (IOException ioe) {
	             LOGGER.error(
	                     "Unexpected IOException encountered while generating "
	                     + "the [ " 
	                     + HashType.SHA1.getText() 
	                     + " ] hash for file [ "
	                     + getInputFile().toString()
	                     + " ].  Exception message => [ "
	                     + ioe.getMessage()
	                     + " ].  Method will return a null hash.");
	        }
        }
	    else {
	    	LOGGER.error("Input file Path is null.  Hash will not be generated.");
	    }
        return hash;
    }

    /**
     * Calculate the SHA-256 hash using the Apache Commons Codec classes.
     * 
     * @param file The file we need the hash for.
     * @return The calculated SHA256 hash.
     */
    public String getSHA256Hash() {

        String          hash = null;
        
        if (getInputFile() != null) {
        	try (InputStream is = Files.newInputStream(Paths.get(getInputFile()))) {
	            hash = org.apache.commons.codec.digest.DigestUtils.sha256Hex(is);
	        }
	        catch (IOException ioe) {
	            LOGGER.error(
	                "Unexpected IOException encountered while generating "
	                + "the [ " 
	                + HashType.SHA256.getText() 
	                + " ] hash for file [ "
	                + getInputFile().toString()
	                + " ].  Exception message => [ "
	                + ioe.getMessage()
	                + " ].  Method will return a null hash.");
	        }
	    }
	    else {
	    	LOGGER.error("Input file Path is null.  Hash will not be generated.");
	    }
        return hash;
    }
    
    /**
     * Calculate the SHA-384 hash using the Apache Commons Codec classes.
     * 
     * @param file The file we need the hash for.
     * @return The calculated SHA384 hash.
     */
    public String getSHA384Hash() {

        String          hash = null;

        if (getInputFile() != null) {
	        try (InputStream is = Files.newInputStream(Paths.get(getInputFile()))) {
	            hash = org.apache.commons.codec.digest.DigestUtils.sha384Hex(is);
	        }
	        catch (IOException ioe) {
	            LOGGER.error(
	                "Unexpected IOException encountered while generating "
	                + "the [ " 
	                + HashType.SHA384.getText() 
	                + " ] hash for file [ "
	                + getInputFile().toString()
	                + " ].  Exception message => [ "
	                + ioe.getMessage()
	                + " ].  Method will return a null hash.");
	        }
        }
        else {
        	LOGGER.error("Input file Path is null.  Hash will not be generated.");
        }
        
        return hash;
    }
    
    /**
     * Calculate the SHA-512 hash using the Apache Commons Codec classes.
     * 
     * @param file The file we need the hash for.
     * @return The calculated SHA512 hash.
     */
    public String getSHA512Hash() {

        String          hash = null;

        if (getInputFile() != null) {
	        try (InputStream is = Files.newInputStream(Paths.get(getInputFile()))) {
	            hash = org.apache.commons.codec.digest.DigestUtils.sha512Hex(is);
	        }
	        catch (IOException ioe) {
	            LOGGER.error(
	                "Unexpected IOException encountered while generating "
	                + "the [ " 
	                + HashType.SHA512.getText() 
	                + " ] hash for file [ "
	                + getInputFile().toString()
	                + " ].  Exception message => [ "
	                + ioe.getMessage()
	                + " ].  Method will return a null hash.");
	        }
        }
        else {
        	LOGGER.error("Input file Path is null.  Hash will not be generated.");
        }
        return hash;
    }
    
    /**
     * Save the calculated hash to the specified output file.
     * 
     * @param hash The calculated hash.
     * @param filename The filename in which to save the hash.
     */
    public void saveHash(String hash) {

        if (getOutputFile() != null) {
	        try (BufferedWriter writer = 
	        		Files.newBufferedWriter(
	        				Paths.get(getOutputFile()), 
	        				Charset.forName("UTF-8"))) {
	            writer.write(hash);
	            writer.flush();
	        }
	        catch (IOException ioe) {
	        	LOGGER.error("Unexpected IOException exception "
	                    + "encountered while attempting to save the hash "
	                    + "String to an output file.  Exception message => [  "
	                    + ioe.getMessage()
	                    + " ].");
	        }
        }
        else {
        	LOGGER.error("Path to the output hash file is null.  Generated "
        			+ "cannot be saved to an on-disk file.");
        }
    }

    /**
     * Internal static class implementing the Builder creation pattern for 
     * new BundleService objects.  
     * 
     * @author L. Craig Carpenter
     */
    public static class HashGeneratorServiceBuilder {
    
    	// Private internal members.
    	private URI      inputFile;
    	private URI      outputFile;
    	private HashType hashType = HashType.SHA1;
    	
        /**
         * Construct a new HashGeneratorService object.  
         * 
         * @return A constructed and validated HashGeneratorService object.
         * @throws IllegalStateException Thrown if any of the input data is 
         * out of range.  
         */
        public HashGeneratorService build() {
        	HashGeneratorService message = new HashGeneratorService(this);
        	validateHashGeneratorServiceObject(message);
            return message;
        }
        
        /**
         * Setter method for type of hash to generate.
         * 
         * @param value The hash type to generate.
         */
        public HashGeneratorServiceBuilder hashType(HashType value) {
        	hashType = value;
        	return this;
        }
        
        /**
         * Setter method for the input file.
         * 
         * @param value The input file
         */
        public HashGeneratorServiceBuilder inputFile(String value) {
        	if ((value != null) && (!value.isEmpty())) {
        		inputFile = URIUtils.getInstance().getURI(value);
        	}
        	return this;
        }
        
        /**
         * Setter method for the output file.
         * 
         * @param value The output file
         */
        public HashGeneratorServiceBuilder outputFile(String value) {
        	if ((value != null) && (!value.isEmpty())) {
        		outputFile = URIUtils.getInstance().getURI(value);
        	}
        	return this;
        }
        
        /**
         * Validate internal member variables.  
         * 
         * @param object The <code>ArchiveMessage</code> object to validate.
         * @throws IllegalStateException Thrown if any of the required fields 
         * are not populated.
         */
        private void validateHashGeneratorServiceObject(
        		HashGeneratorService object) throws IllegalStateException {
        	if (object.getInputFile() == null) {
        		throw new IllegalStateException("The target input file is "
        				+ "not defined.");
        	}
        	if (!Files.exists(Paths.get(object.getInputFile()))) {
        		throw new IllegalStateException("The target input file does "
        				+ "not exist.  Input file specified [ "
        				+ object.getInputFile().toString()
        				+ " ].");
        	}
        	if (object.getOutputFile() == null) {
        		throw new IllegalStateException("The target output file is "
        				+ "not defined.");
        	}
        }
    }
}
