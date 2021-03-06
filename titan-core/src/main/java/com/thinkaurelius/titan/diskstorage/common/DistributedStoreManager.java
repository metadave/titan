package com.thinkaurelius.titan.diskstorage.common;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanConfigurationException;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.Random;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class DistributedStoreManager {

    private static final Logger log = LoggerFactory.getLogger(DistributedStoreManager.class);
    private static final Random random = new Random();

    protected final byte[] rid;

    protected final String[] hostnames;
    protected final int port;
    protected final int connectionTimeout;
    protected final int connectionPoolSize;
//    protected final boolean isKeyOrdered;

    public DistributedStoreManager(Configuration storageConfig, int portDefault) {
        if (storageConfig.containsKey(GraphDatabaseConfiguration.HOSTNAME_KEY)) {
            this.hostnames = storageConfig.getStringArray(GraphDatabaseConfiguration.HOSTNAME_KEY);
        } else {
            this.hostnames = new String[]{GraphDatabaseConfiguration.HOSTNAME_DEFAULT};
        }
        Preconditions.checkArgument(hostnames.length>0,"No hostname configured");
        this.port = storageConfig.getInt(GraphDatabaseConfiguration.PORT_KEY, portDefault);
        this.rid = getRid(storageConfig);
        this.connectionTimeout = storageConfig.getInt(GraphDatabaseConfiguration.CONNECTION_TIMEOUT_KEY, GraphDatabaseConfiguration.CONNECTION_TIMEOUT_DEFAULT);
        this.connectionPoolSize = storageConfig.getInt(GraphDatabaseConfiguration.CONNECTION_POOL_SIZE_KEY, GraphDatabaseConfiguration.CONNECTION_POOL_SIZE_DEFAULT);
        //this.isKeyOrdered = storageConfig.getBoolean(GraphDatabaseConfiguration.STORAGE_IS_ORDERED_KEY,GraphDatabaseConfiguration.STORAGE_IS_ORDERED_DEFAULT);
    }

    protected String getSingleHostname() {
        return hostnames[random.nextInt(hostnames.length)];
    }

    @Override
    public String toString() {
        String hn = getSingleHostname();
        return hn.substring(0, Math.min(hn.length(), 256)) + ":" + port;
    }

    /**
     * This method attempts to generate Rid in the following three ways, in order,
     * returning the value produced by the first successful attempt in the sequence.
     * <p/>
     * <ol>
     * <li>
     * If {@code config} contains {@see GraphDatabaseConfiguration#INSTANCE_RID_RAW_KEY},
     * then read it as a String value.  Convert the String returned into a char[] and
     * call {@code org.apache.commons.codec.binary.Hex#decodeHex on the char[]}.  The
     * byte[] returned by {@code decodeHex} is then returned as Rid.
     * </li>
     * <li>
     * If {@code config} contains {@see GraphDatabaseConfiguration#INSTANCE_RID_SHORT_KEY},
     * then read it as a short value.  Call {@see java.net.InetAddress#getLocalHost()},
     * and on its return value call {@see java.net.InetAddress#getAddress()} to retrieve
     * the machine's IP address in byte[] form.  The returned Rid is a byte[] containing
     * the localhost address bytes in its lower indices and the short value in its
     * penultimate and final indices.
     * </li>
     * <li>
     * If both of the previous failed, then call
     * {@see java.lang.management.RuntimeMXBean#getName()} and then call
     * {@code String#getBytes()} on the returned value.  Return a Rid as described in the
     * previous point, replacing the short value with the byte[] representing the JVM name.
     * </li>
     * </ol>
     *
     * @param config commons config from which to read Rid-related keys
     * @return A byte array which should uniquely identify this machine
     */
    public static byte[] getRid(Configuration config) {

        byte tentativeRid[] = null;

        if (config.containsKey(GraphDatabaseConfiguration.INSTANCE_RID_RAW_KEY)) {
            String ridText =
                    config.getString(GraphDatabaseConfiguration.INSTANCE_RID_RAW_KEY);
            try {
                tentativeRid = Hex.decodeHex(ridText.toCharArray());
            } catch (DecoderException e) {
                throw new TitanConfigurationException("Could not decode hex value", e);
            }

            log.debug("Set rid from hex string: 0x{}", ridText);
        } else {
            final byte[] endBytes;

            if (config.containsKey(GraphDatabaseConfiguration.INSTANCE_RID_SHORT_KEY)) {

                short s = config.getShort(
                        GraphDatabaseConfiguration.INSTANCE_RID_SHORT_KEY);

                endBytes = new byte[2];

                endBytes[0] = (byte) ((s & 0x0000FF00) >> 8);
                endBytes[1] = (byte) (s & 0x000000FF);
            } else {

                endBytes =
                        ManagementFactory.getRuntimeMXBean().getName().getBytes();
            }

            byte[] addrBytes;
            try {
                addrBytes = Inet4Address.getLocalHost().getAddress();
            } catch (UnknownHostException e) {
                throw new TitanConfigurationException("Unknown host specified", e);
            }

            tentativeRid = new byte[addrBytes.length + endBytes.length];
            System.arraycopy(addrBytes, 0, tentativeRid, 0, addrBytes.length);
            System.arraycopy(endBytes, 0, tentativeRid, addrBytes.length, endBytes.length);

            if (log.isDebugEnabled()) {
                log.debug("Set rid: 0x{}", new String(Hex.encodeHex(tentativeRid)));
            }
        }

        return tentativeRid;
    }

}
