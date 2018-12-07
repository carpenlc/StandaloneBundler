package mil.nga;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.bundler.interfaces.BundlerConstantsI;

/**
 * This class was added to ensure that the Hibernate/JPA EntityManager
 * is created. 
 * 
 * @author L. Craig Carpenter
 *
 */
@WebListener
public class PersistenceListener implements ServletContextListener, BundlerConstantsI {

    /**
     * Set up the Log4j system for use throughout the class
     */        
    private static final Logger LOGGER = LoggerFactory.getLogger(
            Bundler.class);
    
	private EntityManagerFactory emf;
	
	public void contextInitialized(ServletContextEvent sce) {
		LOGGER.info("Creating EntityManagerFactory...");
		ServletContext context = sce.getServletContext();
		emf = Persistence.createEntityManagerFactory(APPLICATION_PERSISTENCE_CONTEXT);
	}
	
	public void contextDestroyed(ServletContextEvent sce) {
		LOGGER.info("Closing EntityManagerFactory...");
		if (emf != null) {
			emf.close();
		}
	}
	
}
