/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.heron.utils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.heron.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {
    public static final Logger LOG = LoggerFactory.getLogger(Utils.class);
    public static final String DEFAULT_STREAM_ID = "default";
    private static final Set<Class<?>> defaultAllowedExceptions = Collections.emptySet();
    private static final List<String> LOCALHOST_ADDRESSES = Lists
        .newArrayList("localhost", "127.0.0.1", "0:0:0:0:0:0:0:1");
    private static ClassLoader cl = null;
    private static Map<String, Object> localConf;
    // A singleton instance allows us to mock delegated static methods in our
    // tests by subclassing.
    private static org.apache.heron.utils.Utils _instance = new Utils();
    private static String memoizedLocalHostnameString = null;
    public static final Pattern BLOB_KEY_PATTERN =
            Pattern.compile("^[\\w \\t\\._-]+$", Pattern.UNICODE_CHARACTER_CLASS);
    private static final Pattern TOPOLOGY_NAME_REGEX = Pattern.compile("^[^/.:\\\\]+$");

    /**
     * Provide an instance of this class for delegates to use.  To mock out delegated methods, provide an instance of a subclass that
     * overrides the implementation of the delegated method.
     *
     * @param u a Utils instance
     * @return the previously set instance
     */
    public static Utils setInstance(Utils u) {
        Utils oldInstance = _instance;
        _instance = u;
        return oldInstance;
    }

    @VisibleForTesting
    public static void setClassLoaderForJavaDeSerialize(ClassLoader cl) {
        Utils.cl = cl;
    }

    @VisibleForTesting
    public static void resetClassLoaderForJavaDeSerialize() {
        Utils.cl = ClassLoader.getSystemClassLoader();
    }

    public static List<URL> findResources(String name) {
        try {
            Enumeration<URL> resources = Thread.currentThread().getContextClassLoader().getResources(name);
            List<URL> ret = new ArrayList<URL>();
            while (resources.hasMoreElements()) {
                ret.add(resources.nextElement());
            }
            return ret;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static InputStream getConfigFileInputStream(String configFilePath)
        throws IOException {
        if (null == configFilePath) {
            throw new IOException(
                "Could not find config file, name not specified");
        }

        HashSet<URL> resources = new HashSet<URL>(findResources(configFilePath));
        if (resources.isEmpty()) {
            File configFile = new File(configFilePath);
            if (configFile.exists()) {
                return new FileInputStream(configFile);
            }
        } else if (resources.size() > 1) {
            throw new IOException(
                "Found multiple " + configFilePath
                + " resources. You're probably bundling the Storm jars with your topology jar. "
                + resources);
        } else {
            LOG.debug("Using " + configFilePath + " from resources");
            URL resource = resources.iterator().next();
            return resource.openStream();
        }
        return null;
    }

    /**
     * URL encode the given string using the UTF-8 charset. Once Storm is baselined to Java 11, we can use URLEncoder.encode(String,
     * Charset) instead, which obsoletes this method.
     */
    public static String urlEncodeUtf8(String s) {
        try {
            return URLEncoder.encode(s, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            //This cannot happen since we're using a standard charset
            throw Utils.wrapInRuntime(e);
        }
    }
    
    /**
     * URL decode the given string using the UTF-8 charset. Once Storm is baselined to Java 11, we can use URLDecoder.decode(String,
     * Charset) instead, which obsoletes this method.
     */
    public static String urlDecodeUtf8(String s) {
        try {
            //Once Storm is baselined to Java 11, we can use URLDecoder.decode(String, Charset) instead, which obsoletes this method.
            return URLDecoder.decode(s, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            //This cannot happen since we're using a standard charset
            throw Utils.wrapInRuntime(e);
        }
    }

    public static long bitXorVals(List<Long> coll) {
        long result = 0;
        for (Long val : coll) {
            result ^= val;
        }
        return result;
    }

    public static long bitXor(Long a, Long b) {
        return a ^ b;
    }

    /**
     * Adds the user supplied function as a shutdown hook for cleanup. Also adds a function that sleeps for a second and then halts the
     * runtime to avoid any zombie process in case cleanup function hangs.
     */
    public static void addShutdownHookWithForceKillIn1Sec(Runnable func) {
        addShutdownHookWithDelayedForceKill(func, 1);
    }

    /**
     * Adds the user supplied function as a shutdown hook for cleanup. Also adds a function that sleeps for numSecs and then halts the
     * runtime to avoid any zombie process in case cleanup function hangs.
     */
    public static void addShutdownHookWithDelayedForceKill(Runnable func, int numSecs) {
        final Thread sleepKill = new Thread(() -> {
            try {
                LOG.info("Halting after {} seconds", numSecs);
                Time.sleepSecs(numSecs);
                LOG.warn("Forcing Halt... {}", Utils.threadDump());
                Runtime.getRuntime().halt(20);
            } catch (InterruptedException ie) {
                //Ignored/expected...
            } catch (Exception e) {
                LOG.warn("Exception in the ShutDownHook", e);
            }
        });
        sleepKill.setDaemon(true);
        Thread wrappedFunc = new Thread(() -> {
            func.run();
            sleepKill.interrupt();
        });
        Runtime.getRuntime().addShutdownHook(wrappedFunc);
        Runtime.getRuntime().addShutdownHook(sleepKill);
    }

    public static boolean isSystemId(String id) {
        return id.startsWith("__");
    }

    /**
     * Creates a thread that calls the given code repeatedly, sleeping for an interval of seconds equal to the return value of the previous
     * call.
     *
     * <p>The given afn may be a callable that returns the number of seconds to sleep, or it may be a Callable that returns another Callable
     * that in turn returns the number of seconds to sleep. In the latter case isFactory.
     *
     * @param afn              the code to call on each iteration
     * @param isDaemon         whether the new thread should be a daemon thread
     * @param eh               code to call when afn throws an exception
     * @param priority         the new thread's priority
     * @param isFactory        whether afn returns a callable instead of sleep seconds
     * @param startImmediately whether to start the thread before returning
     * @param threadName       a suffix to be appended to the thread name
     * @return the newly created thread
     *
     * @see Thread
     */
    public static SmartThread asyncLoop(final Callable afn, boolean isDaemon, final UncaughtExceptionHandler eh,
                                        int priority, final boolean isFactory, boolean startImmediately,
                                        String threadName) {
        SmartThread thread = new SmartThread(new Runnable() {
            public void run() {
                try {
                    final Callable<Long> fn = isFactory ? (Callable<Long>) afn.call() : afn;
                    while (true) {
                        if (Thread.interrupted()) {
                            throw new InterruptedException();
                        }
                        final Long s = fn.call();
                        if (s == null) { // then stop running it
                            break;
                        }
                        if (s > 0) {
                            Time.sleep(s);
                        }
                    }
                } catch (Throwable t) {
                    if (Utils.exceptionCauseIsInstanceOf(
                        InterruptedException.class, t)) {
                        LOG.info("Async loop interrupted!");
                        return;
                    }
                    LOG.error("Async loop died!", t);
                    throw new RuntimeException(t);
                }
            }
        });
        if (eh != null) {
            thread.setUncaughtExceptionHandler(eh);
        } else {
            thread.setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
                public void uncaughtException(Thread t, Throwable e) {
                    LOG.error("Async loop died!", e);
                    Utils.exitProcess(1, "Async loop died!");
                }
            });
        }
        thread.setDaemon(isDaemon);
        thread.setPriority(priority);
        if (threadName != null && !threadName.isEmpty()) {
            thread.setName(thread.getName() + "-" + threadName);
        }
        if (startImmediately) {
            thread.start();
        }
        return thread;
    }

    /**
     * Convenience method used when only the function and name suffix are given.
     *
     * @param afn        the code to call on each iteration
     * @param threadName a suffix to be appended to the thread name
     * @return the newly created thread
     *
     * @see Thread
     */
    public static SmartThread asyncLoop(final Callable afn, String threadName, final UncaughtExceptionHandler eh) {
        return asyncLoop(afn, false, eh, Thread.NORM_PRIORITY, false, true,
                         threadName);
    }

    /**
     * Convenience method used when only the function is given.
     *
     * @param afn the code to call on each iteration
     * @return the newly created thread
     */
    public static SmartThread asyncLoop(final Callable afn) {
        return asyncLoop(afn, false, null, Thread.NORM_PRIORITY, false, true,
                         null);
    }

    /**
     * Checks if a throwable is an instance of a particular class.
     *
     * @param klass     The class you're expecting
     * @param throwable The throwable you expect to be an instance of klass
     * @return true if throwable is instance of klass, false otherwise.
     */
    public static boolean exceptionCauseIsInstanceOf(Class klass, Throwable throwable) {
        return unwrapTo(klass, throwable) != null;
    }

    public static <T extends Throwable> T unwrapTo(Class<T> klass, Throwable t) {
        while (t != null) {
            if (klass.isInstance(t)) {
                return (T) t;
            }
            t = t.getCause();
        }
        return null;
    }

    public static <T extends Throwable> void unwrapAndThrow(Class<T> klass, Throwable t) throws T {
        T ret = unwrapTo(klass, t);
        if (ret != null) {
            throw ret;
        }
    }

    public static RuntimeException wrapInRuntime(Exception e) {
        if (e instanceof RuntimeException) {
            return (RuntimeException) e;
        } else {
            return new RuntimeException(e);
        }
    }

    public static long secureRandomLong() {
        return UUID.randomUUID().getLeastSignificantBits();
    }

    /**
     * Gets the storm.local.hostname value, or tries to figure out the local hostname if it is not set in the config.
     *
     * @return a string representation of the hostname.
     */
    public static String hostname() throws UnknownHostException {
        return _instance.hostnameImpl();
    }

    public static String localHostname() throws UnknownHostException {
        return _instance.localHostnameImpl();
    }

    public static void exitProcess(int val, String msg) {
        String combinedErrorMessage = "Halting process: " + msg;
        LOG.error(combinedErrorMessage, new RuntimeException(combinedErrorMessage));
        Runtime.getRuntime().exit(val);
    }

    public static String uuid() {
        return UUID.randomUUID().toString();
    }

    public static byte[] javaSerialize(Object obj) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(obj);
            oos.close();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static <S, T> T get(Map<S, T> m, S key, T def) {
        T ret = m.get(key);
        if (ret == null) {
            ret = def;
        }
        return ret;
    }

    public static double zeroIfNaNOrInf(double x) {
        return (Double.isNaN(x) || Double.isInfinite(x)) ? 0.0 : x;
    }

    public static <T> String join(Iterable<T> coll, String sep) {
        Iterator<T> it = coll.iterator();
        StringBuilder ret = new StringBuilder();
        while (it.hasNext()) {
            ret.append(it.next());
            if (it.hasNext()) {
                ret.append(sep);
            }
        }
        return ret.toString();
    }

    /**
     * Is the topology configured to have ZooKeeper authentication.
     *
     * @param conf the topology configuration
     * @return true if ZK is configured else false
     */
    public static boolean isZkAuthenticationConfiguredTopology(Map<String, Object> conf) {
        return (conf != null
                && conf.get(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_SCHEME) != null
                && !((String) conf.get(Config.STORM_ZOOKEEPER_TOPOLOGY_AUTH_SCHEME)).isEmpty());
    }

    public static void handleUncaughtException(Throwable t) {
        handleUncaughtException(t, defaultAllowedExceptions);
    }

    public static void handleUncaughtException(Throwable t, Set<Class<?>> allowedExceptions) {
        if (t != null) {
            if (t instanceof OutOfMemoryError) {
                try {
                    System.err.println("Halting due to Out Of Memory Error..." + Thread.currentThread().getName());
                } catch (Throwable err) {
                    //Again we don't want to exit because of logging issues.
                }
                Runtime.getRuntime().halt(-1);
            }
        }

        if (allowedExceptions.contains(t.getClass())) {
            LOG.info("Swallowing {} {}", t.getClass(), t);
            return;
        }

        //Running in daemon mode, we would pass Error to calling thread.
        throw new Error(t);
    }

    public static void sleepNoSimulation(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
    
    public static void sleep(long millis) {
        try {
            Time.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public static UptimeComputer makeUptimeComputer() {
        return _instance.makeUptimeComputerImpl();
    }

    /**
     * <code>"{:a 1 :b 1 :c 2} -> {1 [:a :b] 2 :c}"</code>.
     *
     * <p>Example usage in java:
     * <code>Map&lt;Integer, String&gt; tasks; Map&lt;String, List&lt;Integer&gt;&gt; componentTasks = Utils.reverse_map(tasks);</code>
     *
     * <p>The order of he resulting list values depends on the ordering properties of the Map passed in. The caller is
     * responsible for passing an ordered map if they expect the result to be consistently ordered as well.
     *
     * @param map to reverse
     * @return a reversed map
     */
    public static <K, V> HashMap<V, List<K>> reverseMap(Map<K, V> map) {
        HashMap<V, List<K>> rtn = new HashMap<V, List<K>>();
        if (map == null) {
            return rtn;
        }
        for (Entry<K, V> entry : map.entrySet()) {
            K key = entry.getKey();
            V val = entry.getValue();
            List<K> list = rtn.get(val);
            if (list == null) {
                list = new ArrayList<K>();
                rtn.put(entry.getValue(), list);
            }
            list.add(key);
        }
        return rtn;
    }

    /**
     * "[[:a 1] [:b 1] [:c 2]} -> {1 [:a :b] 2 :c}" Reverses an assoc-list style Map like reverseMap(Map...)
     *
     * @param listSeq to reverse
     * @return a reversed map
     */
    public static Map<Object, List<Object>> reverseMap(List<List<Object>> listSeq) {
        Map<Object, List<Object>> rtn = new HashMap<>();
        if (listSeq == null) {
            return rtn;
        }
        for (List<Object> listEntry : listSeq) {
            Object key = listEntry.get(0);
            Object val = listEntry.get(1);
            List<Object> list = rtn.get(val);
            if (list == null) {
                list = new ArrayList<>();
                rtn.put(val, list);
            }
            list.add(key);
        }
        return rtn;
    }

    public static boolean isOnWindows() {
        if (System.getenv("OS") != null) {
            return System.getenv("OS").equals("Windows_NT");
        }
        return false;
    }

    public static boolean checkFileExists(String path) {
        return Files.exists(new File(path).toPath());
    }

    public static byte[] toByteArray(ByteBuffer buffer) {
        byte[] ret = new byte[buffer.remaining()];
        buffer.get(ret, 0, ret.length);
        return ret;
    }

    public static Runnable mkSuicideFn() {
        return new Runnable() {
            @Override
            public void run() {
                exitProcess(1, "Worker died");
            }
        };
    }

    public static void readAndLogStream(String prefix, InputStream in) {
        try {
            BufferedReader r = new BufferedReader(new InputStreamReader(in));
            String line = null;
            while ((line = r.readLine()) != null) {
                LOG.info("{}:{}", prefix, line);
            }
        } catch (IOException e) {
            LOG.warn("Error while trying to log stream", e);
        }
    }

    public static List<Object> tuple(Object... values) {
        List<Object> ret = new ArrayList<Object>();
        for (Object v : values) {
            ret.add(v);
        }
        return ret;
    }

    public static byte[] gzip(byte[] data) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            GZIPOutputStream out = new GZIPOutputStream(bos);
            out.write(data);
            out.close();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static byte[] gunzip(byte[] data) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ByteArrayInputStream bis = new ByteArrayInputStream(data);
            GZIPInputStream in = new GZIPInputStream(bis);
            byte[] buffer = new byte[1024];
            int len = 0;
            while ((len = in.read(buffer)) >= 0) {
                bos.write(buffer, 0, len);
            }
            in.close();
            bos.close();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<String> getRepeat(List<String> list) {
        List<String> rtn = new ArrayList<String>();
        Set<String> idSet = new HashSet<String>();

        for (String id : list) {
            if (idSet.contains(id)) {
                rtn.add(id);
            } else {
                idSet.add(id);
            }
        }

        return rtn;
    }

    /**
     * A cheap way to deterministically convert a number to a positive value. When the input is positive, the original value is returned.
     * When the input number is negative, the returned positive value is the original value bit AND against Integer.MAX_VALUE(0x7fffffff)
     * which is not its absolutely value.
     *
     * @param number a given number
     * @return a positive number.
     */
    public static int toPositive(int number) {
        return number & Integer.MAX_VALUE;
    }

    /**
     * Get process PID.
     * @return the pid of this JVM, because Java doesn't provide a real way to do this.
     */
    public static String processPid() {
        String name = ManagementFactory.getRuntimeMXBean().getName();
        String[] split = name.split("@");
        if (split.length != 2) {
            throw new RuntimeException("Got unexpected process name: " + name);
        }
        return split[0];
    }

    /**
     * Creates a new map with a string value in the map replaced with an equivalently-lengthed string of '#'.  (If the object is not a
     * string to string will be called on it and replaced)
     *
     * @param m   The map that a value will be redacted from
     * @param key The key pointing to the value to be redacted
     * @return a new map with the value redacted. The original map will not be modified.
     */
    public static Map<String, Object> redactValue(Map<String, Object> m, String key) {
        if (m.containsKey(key)) {
            HashMap<String, Object> newMap = new HashMap<>(m);
            Object value = newMap.get(key);
            String v = value.toString();
            String redacted = new String(new char[v.length()]).replace("\0", "#");
            newMap.put(key, redacted);
            return newMap;
        }
        return m;
    }

    public static UncaughtExceptionHandler createDefaultUncaughtExceptionHandler() {
        return (thread, thrown) -> {
            try {
                handleUncaughtException(thrown);
            } catch (Error err) {
                LOG.error("Received error in thread {}.. terminating server...", thread.getName(), err);
                Runtime.getRuntime().exit(-2);
            }
        };
    }
    
    public static void setupDefaultUncaughtExceptionHandler() {
        Thread.setDefaultUncaughtExceptionHandler(createDefaultUncaughtExceptionHandler());
    }

    /**
     * parses the arguments to extract jvm heap memory size in MB.
     *
     * @return the value of the JVM heap memory setting (in MB) in a java command.
     */
    public static Double parseJvmHeapMemByChildOpts(List<String> options, Double defaultValue) {
        if (options != null) {
            Pattern optsPattern = Pattern.compile("Xmx([0-9]+)([mkgMKG])");
            for (String option : options) {
                if (option == null) {
                    continue;
                }
                Matcher m = optsPattern.matcher(option);
                while (m.find()) {
                    int value = Integer.parseInt(m.group(1));
                    char unitChar = m.group(2).toLowerCase().charAt(0);
                    int unit;
                    switch (unitChar) {
                        case 'k':
                            unit = 1024;
                            break;
                        case 'm':
                            unit = 1024 * 1024;
                            break;
                        case 'g':
                            unit = 1024 * 1024 * 1024;
                            break;
                        default:
                            unit = 1;
                    }
                    Double result = value * unit / 1024.0 / 1024.0;
                    return (result < 1.0) ? 1.0 : result;
                }
            }
            return defaultValue;
        } else {
            return defaultValue;
        }
    }

    @SuppressWarnings("unchecked")
    private static Object normalizeConfValue(Object obj) {
        if (obj instanceof Map) {
            return normalizeConf((Map<String, Object>) obj);
        } else if (obj instanceof Collection) {
            List<Object> confList = new ArrayList<>((Collection<Object>) obj);
            for (int i = 0; i < confList.size(); i++) {
                Object val = confList.get(i);
                confList.set(i, normalizeConfValue(val));
            }
            return confList;
        } else if (obj instanceof Integer) {
            return ((Number) obj).longValue();
        } else if (obj instanceof Float) {
            return ((Float) obj).doubleValue();
        } else {
            return obj;
        }
    }

    private static Map<String, Object> normalizeConf(Map<String, Object> conf) {
        if (conf == null) {
            return new HashMap<>();
        }
        Map<String, Object> ret = new HashMap<>(conf);
        for (Entry<String, Object> entry : ret.entrySet()) {
            ret.put(entry.getKey(), normalizeConfValue(entry.getValue()));
        }
        return ret;
    }

    /**
     * Gets some information, including stack trace, for a running thread.
     *
     * @return A human-readable string of the dump.
     */
    public static String threadDump() {
        final StringBuilder dump = new StringBuilder();
        final java.lang.management.ThreadMXBean threadMxBean = ManagementFactory.getThreadMXBean();
        final ThreadInfo[] threadInfos = threadMxBean.getThreadInfo(threadMxBean.getAllThreadIds(), 100);
        for (Entry<Thread, StackTraceElement[]> entry: Thread.getAllStackTraces().entrySet()) {
            Thread t = entry.getKey();
            ThreadInfo threadInfo = threadMxBean.getThreadInfo(t.getId());
            if (threadInfo == null) {
                //Thread died before we could get the info, skip
                continue;
            }
            dump.append('"');
            dump.append(threadInfo.getThreadName());
            dump.append("\" ");
            if (t.isDaemon()) {
                dump.append("(DAEMON)");
            }
            dump.append("\n   lock: ");
            dump.append(threadInfo.getLockName());
            dump.append(" owner: ");
            dump.append(threadInfo.getLockOwnerName());
            final Thread.State state = threadInfo.getThreadState();
            dump.append("\n   java.lang.Thread.State: ");
            dump.append(state);
            for (final StackTraceElement stackTraceElement : entry.getValue()) {
                dump.append("\n        at ");
                dump.append(stackTraceElement);
            }
            dump.append("\n\n");
        }
        return dump.toString();
    }

    public static boolean checkDirExists(String dir) {
        File file = new File(dir);
        return file.isDirectory();
    }

    /**
     * Is the cluster configured to interact with ZooKeeper in a secure way? This only works when called from within Nimbus or a Supervisor
     * process.
     *
     * @param conf the storm configuration, not the topology configuration
     * @return true if it is configured else false.
     */
    public static boolean isZkAuthenticationConfiguredStormServer(Map<String, Object> conf) {
        return null != System.getProperty("java.security.auth.login.config")
               || (conf != null
                   && conf.get(Config.STORM_ZOOKEEPER_AUTH_SCHEME) != null
                   && !((String) conf.get(Config.STORM_ZOOKEEPER_AUTH_SCHEME)).isEmpty());
    }

    public static double nullToZero(Double v) {
        return (v != null ? v : 0);
    }

    /**
     * a or b the first one that is not null.
     *
     * @param a something
     * @param b something else
     * @return a or b the first one that is not null
     */
    @SuppressWarnings({ "checkstyle:AbbreviationAsWordInName", "checkstyle:MethodName"})
    public static <V> V OR(V a, V b) {
        return a == null ? b : a;
    }

    public static TreeMap<Integer, Integer> integerDivided(int sum, int numPieces) {
        int base = sum / numPieces;
        int numInc = sum % numPieces;
        int numBases = numPieces - numInc;
        TreeMap<Integer, Integer> ret = new TreeMap<Integer, Integer>();
        ret.put(base, numBases);
        if (numInc != 0) {
            ret.put(base + 1, numInc);
        }
        return ret;
    }

    /**
     * Fills up chunks out of a collection (given a maximum amount of chunks).
     *
     * <p>i.e. partitionFixed(5, [1,2,3]) -> [[1,2,3]] partitionFixed(5, [1..9]) -> [[1,2], [3,4], [5,6], [7,8], [9]] partitionFixed(3,
     * [1..10]) -> [[1,2,3,4], [5,6,7], [8,9,10]]
     *
     * @param maxNumChunks the maximum number of chunks to return
     * @param coll         the collection to be chunked up
     * @return a list of the chunks, which are themselves lists.
     */
    public static <T> List<List<T>> partitionFixed(int maxNumChunks, Collection<T> coll) {
        List<List<T>> ret = new ArrayList<>();

        if (maxNumChunks == 0 || coll == null) {
            return ret;
        }

        Map<Integer, Integer> parts = integerDivided(coll.size(), maxNumChunks);

        // Keys sorted in descending order
        List<Integer> sortedKeys = new ArrayList<Integer>(parts.keySet());
        Collections.sort(sortedKeys, Collections.reverseOrder());


        Iterator<T> it = coll.iterator();
        for (Integer chunkSize : sortedKeys) {
            if (!it.hasNext()) {
                break;
            }
            Integer times = parts.get(chunkSize);
            for (int i = 0; i < times; i++) {
                if (!it.hasNext()) {
                    break;
                }
                List<T> chunkList = new ArrayList<>();
                for (int j = 0; j < chunkSize; j++) {
                    if (!it.hasNext()) {
                        break;
                    }
                    chunkList.add(it.next());
                }
                ret.add(chunkList);
            }
        }

        return ret;
    }

    /**
     * Gets an available port. Consider if it is possible to pass port 0 to the server instead of using this method, since there is no
     * guarantee that the port returned by this method will remain free.
     *
     * @return The preferred port if available, or a random available port
     */
    public static int getAvailablePort(int preferredPort) {
        int localPort = -1;
        try (ServerSocket socket = new ServerSocket(preferredPort)) {
            localPort = socket.getLocalPort();
        } catch (IOException exp) {
            if (preferredPort > 0) {
                return getAvailablePort(0);
            }
        }
        return localPort;
    }

    /**
     * Shortcut to calling {@link #getAvailablePort(int) } with 0 as the preferred port.
     *
     * @return A random available port
     */
    public static int getAvailablePort() {
        return getAvailablePort(0);
    }

    public static String memoizedLocalHostname() throws UnknownHostException {
        if (memoizedLocalHostnameString == null) {
            memoizedLocalHostnameString = localHostname();
        }
        return memoizedLocalHostnameString;
    }

    public static boolean isLocalhostAddress(String address) {
        return LOCALHOST_ADDRESSES.contains(address);
    }

    public static <K, V> Map<K, V> merge(Map<? extends K, ? extends V> first, Map<? extends K, ? extends V> other) {
        Map<K, V> ret = new HashMap<>(first);
        if (other != null) {
            ret.putAll(other);
        }
        return ret;
    }

    public static <V> ArrayList<V> convertToArray(Map<Integer, V> srcMap, int start) {
        Set<Integer> ids = srcMap.keySet();
        Integer largestId = ids.stream().max(Integer::compareTo).get();
        int end = largestId - start;
        ArrayList<V> result = new ArrayList<>(Collections.nCopies(end + 1, null)); // creates array[largestId+1] filled with nulls
        for (Entry<Integer, V> entry : srcMap.entrySet()) {
            int id = entry.getKey();
            if (id < start) {
                LOG.debug("Entry {} will be skipped it is too small {} ...", id, start);
            } else {
                result.set(id - start, entry.getValue());
            }
        }
        return result;
    }

    // Non-static impl methods exist for mocking purposes.
    public UptimeComputer makeUptimeComputerImpl() {
        return new UptimeComputer();
    }

    // Non-static impl methods exist for mocking purposes.
    protected String localHostnameImpl() throws UnknownHostException {
        return InetAddress.getLocalHost().getCanonicalHostName();
    }

    // Non-static impl methods exist for mocking purposes.
    protected String hostnameImpl() throws UnknownHostException {
        if (localConf == null) {
            return memoizedLocalHostname();
        }
        Object hostnameString = localConf.get(Config.STORM_LOCAL_HOSTNAME);
        if (hostnameString == null || hostnameString.equals("")) {
            return memoizedLocalHostname();
        }
        return (String) hostnameString;
    }

    /**
     * Validates topology name.
     * @param name the topology name
     * @throws IllegalArgumentException if the topology name is not valid
     */
    public static void validateTopologyName(String name) throws IllegalArgumentException {
        if (name == null || !TOPOLOGY_NAME_REGEX.matcher(name).matches()) {
            String message = "Topology name '" + name + "' is not valid. It can't be null and it must match " + TOPOLOGY_NAME_REGEX;
            throw new IllegalArgumentException(message);
        }
    }

    /**
     * A thread that can answer if it is sleeping in the case of simulated time. This class is not useful when simulated time is not being
     * used.
     */
    public static class SmartThread extends Thread {
        public SmartThread(Runnable r) {
            super(r);
        }

        public boolean isSleeping() {
            return Time.isThreadWaiting(this);
        }
    }

    public static class UptimeComputer {
        int startTime = 0;

        public UptimeComputer() {
            startTime = Time.currentTimeSecs();
        }

        public int upTime() {
            return Time.deltaSecs(startTime);
        }
    }

}
