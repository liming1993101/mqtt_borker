package com.sh.wd.spi.impl.security;

import com.sh.wd.config.BrokerConstants;
import com.sh.wd.config.IConfig;
import com.sh.wd.spi.impl.subscriptions.Topic;
import com.sh.wd.spi.security.IAuthorizator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DBAuthorizator implements IAuthorizator {
    
    private static final Logger LOG = LoggerFactory.getLogger(DBAuthorizator.class);
            
    private final MessageDigest messageDigest;
    private final PreparedStatement preparedStatement;
    
    private List<Authorization> m_globalAuthorizations = new ArrayList<>();
    private List<Authorization> m_patternAuthorizations = new ArrayList<>();
    private Map<String, List<Authorization>> m_userAuthorizations = new HashMap<>();
    
    public DBAuthorizator(IConfig config) {
        this(
                config.getProperty(BrokerConstants.DB_AUTHORIZATOR_DRIVER, ""),
                config.getProperty(BrokerConstants.DB_AUTHORIZATOR_URL, ""),
                config.getProperty(BrokerConstants.DB_AUTHORIZATOR_QUERY, ""),
                config.getProperty(BrokerConstants.DB_AUTHORIZATOR_DIGEST, ""));
    }
    
    /**
     * provide authenticator from SQL database
     *
     * @param driver
     *            : jdbc driver class like : "com.mysql.jdbc.Driver"
     * @param jdbcUrl
     *            : jdbc url like : "jdbc:postgresql://host:port/dbname"
     * @param sqlQuery
     *            : sql query like : "SELECT USERNAME, TOPIC, PERMISSION FROM ACL WHERE STATE=?"
     * @param digestMethod
     *            : password encoding algorithm : "MD5", "SHA-1", "SHA-256"
     */
    public DBAuthorizator(String driver, String jdbcUrl, String sqlQuery, String digestMethod) {

        try {
            Class.forName(driver);
            final Connection connection = DriverManager.getConnection(jdbcUrl);
            this.messageDigest = MessageDigest.getInstance(digestMethod);
            this.preparedStatement = connection.prepareStatement(sqlQuery);
        } catch (ClassNotFoundException cnfe) {
            LOG.error(String.format("Can't find driver %s", driver), cnfe);
            throw new RuntimeException(cnfe);
        } catch (SQLException sqle) {
            LOG.error(String.format("Can't connect to %s", jdbcUrl), sqle);
            throw new RuntimeException(sqle);
        } catch (NoSuchAlgorithmException nsaex) {
            LOG.error(String.format("Can't find %s for password encoding", digestMethod), nsaex);
            throw new RuntimeException(nsaex);
        }
    }
    
    @Override
    public boolean canWrite(Topic topic, String user, String client) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean canRead(Topic topic, String user, String client) {
        // TODO Auto-generated method stub
        return false;
    }

}
