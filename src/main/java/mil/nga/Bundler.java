package mil.nga;

import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.bundler.BundleRequest;
import mil.nga.bundler.exceptions.ServiceUnavailableException;
import mil.nga.bundler.interfaces.BundlerConstantsI;
import mil.nga.bundler.messages.BundleRequestMessage;
import mil.nga.bundler.messages.BundlerMessageSerializer;
import mil.nga.bundler.messages.JobTrackerMessage;
import mil.nga.bundler.services.JobFactoryService;
import mil.nga.bundler.services.JobService;
import mil.nga.bundler.services.JobTrackerService;
import mil.nga.bundler.services.RequestArchiveService;
import mil.nga.bundler.types.JobStateType;
import mil.nga.util.FileUtils;

/**
 * The <code>Bundler</code> class serves as the JAX-RS end point for the 
 * Standalone bundler application.  This class is designed to work with the
 * RestEasy JAX-RS implementation. 
 *  
 * @author L. Craig Carpenter
 */
@Path("")
public class Bundler extends PropertyLoader implements BundlerConstantsI {

    /**
     * Set up the Log4j system for use throughout the class
     */        
    private static final Logger LOGGER = LoggerFactory.getLogger(
            Bundler.class);
    /**
     * Common header names in which the client CN is inserted
     */
    public static final String[] CERT_HEADERS = {
        "X-SSL-Client-CN",
        "SSL_CLIENT_S_DN_CN",
        "SM_USER",
        "SM_USER_CN"
    };
    
    /**
     * The name of the application
     */
    public static final String APPLICATION_NAME = "Bundler";
    
    /**
     * Try a couple of different headers to see if we can get a user 
     * name for the incoming request.  About 50% of the time this function 
     * doesn't work because the AJAX callers do not insert the request
     * headers.
     * 
     * @param headers HTTP request headers
     * @return The username if it could be extracted from the headers
     */
    public String getUser(HttpHeaders headers) {
        
        String method = "getUser() - ";
        String user   = null;
        
        if (headers != null) {
            MultivaluedMap<String, String> map = headers.getRequestHeaders();
            for (String key : map.keySet()) {
                for (String header : CERT_HEADERS) {
                    if (header.equalsIgnoreCase(key)) {
                        user = map.get(key).get(0);
                        break;
                    }
                }
            }
        }
        else {
            LOGGER.warn(method 
                    + "HTTP request headers are not available.");
        }
        if ((user == null) || (user.isEmpty())) {
            user = "unavailable";
        }
        return user;
    }
    
    /**
     * Simple method used to determine whether or not the bundler 
     * application is responding to requests.
     */
    @GET
    @HEAD
    @Path("/isAlive")
    public Response isAlive(@Context HttpHeaders headers) {
        StringBuilder sb = new StringBuilder();
        sb.append("Application [ ");
        sb.append(APPLICATION_NAME);
        sb.append(" ] on host [ ");
        sb.append(FileUtils.getHostName());
        sb.append(" ] and called by user [ ");
        sb.append(getUser(headers));
        sb.append(" ] is alive!");
        return Response.status(Status.OK).entity(sb.toString()).build();
    }
    
    /**
     * Alternate version of the bundler entry point allowing clients to 
     * call the bundler with media type of text/plain.  This was implemented 
     * for the Aero folks working on cloud migration.  The "application/json"
     * type was causing an HTTP "options" call that is not handled properly by
     * the authentication software.  This method is identical to the 
     * <code>BundleFilesJSON</code> method below with the additional step of 
     * manually deserializing the input request.
     * 
     * @param headers The HTTP request headers.
     * @param request The incoming JSON bundle request in String format.
     * @return <code>JobTrackerMessage</code> object deserialized to JSON.
     */
    @HEAD
    @POST
    @Path("BundleFilesText")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.APPLICATION_JSON)
    public Response bundleText(
            @Context HttpHeaders headers,
            String requestString) {
        
        BundleRequest     request = null;
        JobTrackerMessage message = null;
        
        if (requestString != null) {
            
            request = BundlerMessageSerializer
                    .getInstance()
                    .deserializeToBundleRequest(requestString);
            
            if (request != null) {
                
                // If the client user name was not set in the request, attempt to 
                // extract it from the input request headers.
                if ((request.getUserName() == null) || 
                        (request.getUserName().isEmpty()) || 
                        (request.getUserName().equalsIgnoreCase(DEFAULT_USERNAME))) {
                    request.setUserName(getUser(headers));
                }
                
                LOGGER.info("Incoming request parsed [ "
                        + request.toString()
                        + " ].");
                
                String jobID = FileUtils.generateUniqueToken(JOB_ID_LENGTH);
            	
            	// Archive the incoming request
            	RequestArchiveService.getInstance().archiveRequest(request, jobID);
            	
            	new JobFactoryService.JobFactoryServiceBuilder()
		    			.jobID(jobID)
		    			.bundleRequest(request)
		    			.build()
		    			.start();

                // Build the return message.
                message = new JobTrackerMessage.JobTrackerMessageBuilder()
                		.jobID(jobID)
                		.userName(request.getUserName())
                		.state(JobStateType.NOT_STARTED)
                		.build();
                
            }
        }
        return Response.ok(message, MediaType.APPLICATION_JSON).build();
    }
    
    /**
     * Main end-point for the <code>Bundler</code> application.  
     * 
     * @param request
     * @return
     */
    @POST
    @HEAD
    @Path("/BundleFilesJSON")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response bundle(
            @Context HttpHeaders headers,
            BundleRequest request) {
        
        JobTrackerMessage message = null;
        
        // Make sure the input request was parsed.
        if (request != null) {
            
            // If the client user name was not set in the request, attempt to 
            // extract it from the input request headers.
            if ((request.getUserName() == null) || 
                    (request.getUserName().isEmpty()) || 
                    (request.getUserName().equalsIgnoreCase(DEFAULT_USERNAME))) {
                request.setUserName(getUser(headers));
            }
            
            if (LOGGER.isDebugEnabled()) {
	            LOGGER.debug("Incoming request parsed [ "
	                    + request.toString()
	                    + " ].");
            }
            
            String jobID = FileUtils.generateUniqueToken(JOB_ID_LENGTH);
        	
        	// Archive the incoming request
        	RequestArchiveService.getInstance().archiveRequest(request, jobID);
        	
        	new JobFactoryService.JobFactoryServiceBuilder()
						.jobID(jobID)
						.bundleRequest(request)
						.build()
						.start();

            // Build the return message.
            message = new JobTrackerMessage.JobTrackerMessageBuilder()
            			.jobID(jobID)
            			.userName(request.getUserName())
            			.state(JobStateType.NOT_STARTED)
            			.build();
            
        }
        return Response.ok(message, MediaType.APPLICATION_JSON).build();
    }
    
    /**
     * End-point used by customers to get real-time status on in-progress bundle
     * jobs.
     * 
     * @param request
     * @return
     */
    @POST
    @HEAD
    @Path("/BundleFiles")
    //@Consumes(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response bundle(
            @Context HttpHeaders headers,
            BundleRequestMessage request) {
        
        JobTrackerMessage message     = null;
        
        // Make sure the input request was parsed.
        if (request != null) {
            
            // If the client user name was not set in the request, attempt to 
            // extract it from the input request headers.
            if ((request.getUserName() == null) || 
                    (request.getUserName().isEmpty()) || 
                    (request.getUserName().equalsIgnoreCase(DEFAULT_USERNAME))) {
                request.setUserName(getUser(headers));
            }
            
            LOGGER.info("Incoming request parsed [ "
                    + request.toString()
                    + " ].");
                
        	String jobID = FileUtils.generateUniqueToken(JOB_ID_LENGTH);
        	
        	// Archive the incoming request
        	RequestArchiveService.getInstance().archiveRequest(request, jobID);
        	
        	new JobFactoryService.JobFactoryServiceBuilder()
        			.jobID(jobID)
        			.bundleRequestMessage(request)
        			.build()
        			.start();

            // Build the return message.
            message = new JobTrackerMessage.JobTrackerMessageBuilder()
            			.jobID(jobID)
            			.userName(request.getUserName())
            			.state(JobStateType.NOT_STARTED)
            			.build();

        }
        return Response.ok(message, MediaType.APPLICATION_JSON).build();
    }
    
    /**
     * Provide status information on the bundle operations associated with the
     * input job id.
     * 
     * @param jobID The ID of the job in question.
     * @return JSON representation of the job status, or error if not found.
     */
    @GET
    @HEAD
    @Path("/GetState")
    @Produces(MediaType.APPLICATION_JSON)
    public JobTrackerMessage getState(
                    @QueryParam("job_id") String jobID) {

        JobTrackerMessage status = null;
        
        if ((jobID != null) && (!jobID.isEmpty())) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Retrieving state for job ID [ "
                        + jobID
                        + " ].");
            }
            try (JobTrackerService service = new JobTrackerService()) {
                status = service.getJobTracker(jobID);
            }
        }
        else {
            String msg =  "Null or empty job_id provided in request.";
            LOGGER.error(msg);
            throw new WebArchiveException(msg);
        }
        return status;
    }
    
    @GET
    @HEAD
    @Path("/DataSourceTest")
    @Produces(MediaType.TEXT_PLAIN)
    public Response dataSourceTest() {
    	
    	String message;
    	
    	try (JobService service = new JobService()) {
    		List<String> jobIDs = service.getJobIDs();
    		if ((jobIDs != null) && (jobIDs.size() > 0)) {
    			StringBuilder sb = new StringBuilder();
    			for (String id : jobIDs) {
    				sb.append(id);
    				sb.append(System.getProperty("line.seperator"));
    			}
    			message = sb.toString();
    		}
    		else {
    			message = "Job ID list is empty.";
    		}
    	}
    	catch (ServiceUnavailableException sue) {
    		LOGGER.error("Unexpected ServiceUnavailableException exception "
    				+ "encountered while retrieving the list of job IDs.  "
    				+ "Exception message => [ "
    				+ sue.getMessage()
    				+ " ].");
    		message = sue.getMessage();
    	}
    	return Response.ok(message, MediaType.TEXT_PLAIN).build();
    }
    
}
