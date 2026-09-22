package org.apache.heron.hdfs.security.auth;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.util.Date;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ClientAuthUtilsKerberosTest {

    private KerberosTicket createSampleTicket(InetAddress[] clientAddresses) {
        byte[] asn1Encoding = new byte[]{1, 2, 3, 4};
        KerberosPrincipal client = new KerberosPrincipal("client@HERON.APACHE.ORG");
        KerberosPrincipal server = new KerberosPrincipal("krbtgt/HERON.APACHE.ORG@HERON.APACHE.ORG");
        byte[] sessionKey = new byte[]{10, 20, 30, 40};
        int keyType = 1;
        boolean[] flags = new boolean[32];
        flags[1] = true; // forwardable
        flags[8] = true; // renewable
        Date now = new Date();
        Date startTime = new Date(now.getTime() - 1000);
        Date endTime = new Date(now.getTime() + 3600000);
        Date renewTill = new Date(now.getTime() + 7200000);

        return new KerberosTicket(
                asn1Encoding, client, server, sessionKey, keyType,
                flags, now, startTime, endTime, renewTill, clientAddresses
        );
    }

    @Test
    public void testRoundtripStandardTicket() throws Exception {
        KerberosTicket original = createSampleTicket(null);
        byte[] serialized = ClientAuthUtils.serializeKerberosTicket(original);
        Assert.assertNotNull(serialized);

        KerberosTicket deserialized = ClientAuthUtils.deserializeKerberosTicket(serialized);
        Assert.assertNotNull(deserialized);
        Assert.assertEquals(deserialized.getClient(), original.getClient());
        Assert.assertEquals(deserialized.getServer(), original.getServer());
        Assert.assertEquals(deserialized.getEndTime(), original.getEndTime());
        Assert.assertEquals(deserialized.getFlags(), original.getFlags());
    }

    @Test
    public void testRoundtripTicketWithClientAddresses() throws Exception {
        InetAddress[] addresses = new InetAddress[]{
                InetAddress.getByName("127.0.0.1"),
                InetAddress.getByName("10.0.0.1")
        };
        KerberosTicket original = createSampleTicket(addresses);
        byte[] serialized = ClientAuthUtils.serializeKerberosTicket(original);
        Assert.assertNotNull(serialized);

        // Verification of ObjectInputFilter accepting InetAddress[]
        KerberosTicket deserialized = ClientAuthUtils.deserializeKerberosTicket(serialized);
        Assert.assertNotNull(deserialized);
        Assert.assertNotNull(deserialized.getClientAddresses());
        Assert.assertEquals(deserialized.getClientAddresses().length, 2);
    }

    @Test
    public void testCloneKerberosTicket() throws Exception {
        Assert.assertNull(ClientAuthUtils.cloneKerberosTicket(null));

        KerberosTicket original = createSampleTicket(null);
        KerberosTicket cloned = ClientAuthUtils.cloneKerberosTicket(original);
        Assert.assertNotNull(cloned);
        Assert.assertNotSame(cloned, original);
        Assert.assertEquals(cloned.getClient(), original.getClient());
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testMaliciousPayloadRejection() throws Exception {
        ByteArrayOutputStream bao = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(bao)) {
            oos.writeObject(new java.awt.Point(10, 20));
        }
        ClientAuthUtils.deserializeKerberosTicket(bao.toByteArray());
    }

    @Test
    public void testNullAndEmptyBytesDeserialization() {
        Assert.assertNull(ClientAuthUtils.deserializeKerberosTicket(null));
        Assert.assertNull(ClientAuthUtils.deserializeKerberosTicket(new byte[0]));
    }
}
