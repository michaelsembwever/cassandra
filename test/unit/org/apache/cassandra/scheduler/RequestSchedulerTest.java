package org.apache.cassandra.scheduler;

import java.lang.management.ManagementFactory;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.config.Config.RequestSchedulerId;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.RequestSchedulerOptions;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.thrift.ThriftClientState;



public class RequestSchedulerTest
{

    private final ExecutorService executor = Executors.newFixedThreadPool(13);
    private IRequestScheduler[] requestSchedulers;

    private void setup(RequestSchedulerOptions options)
    {
        for (String id : new String[] {"unknown", "system", "anonymous"})
        {
            try
            {
                ManagementFactory.getPlatformMBeanServer()
                        .unregisterMBean(new ObjectName("org.apache.cassandra.scheduler:type=WeightedQueue,queue=" + id));
            }
            catch (InstanceNotFoundException | MBeanRegistrationException | MalformedObjectNameException ex)
            {}
        }

        TestDatabaseDescripter.setRequestSchedulerOptions(options);

        requestSchedulers = new IRequestScheduler[]
        {
            new NoScheduler(),
            new RoundRobinScheduler(TestDatabaseDescripter.getRequestSchedulerOptions())
        };
    }

    @Test
    public void test_throughput() throws InterruptedException, ExecutionException
    {
        setup(new RequestSchedulerOptions());
        testImpl("test");
    }

    @Test
    public void test_throughput_with_weights() throws InterruptedException, ExecutionException
    {
        RequestSchedulerOptions options = new RequestSchedulerOptions();
        options.throttle_limit = 1;
        options.weights = new HashMap<>();
        options.weights.put("system", 3);
        options.weights.put("anonymous", 3);
        setup(options);
        testImpl("test_weights");
    }

    @Test(expected = TimeoutException.class)
    public void test_timeout() throws InvalidRequestException, TimeoutException
    {
        RequestSchedulerOptions options = new RequestSchedulerOptions();
        options.throttle_limit = 1;
        options.weights = new HashMap<>();
        options.weights.put("system", 0);
        setup(options);
        TestDatabaseDescripter.setRequestSchedulerId(RequestSchedulerId.keyspace);
        TestDatabaseDescripter.setRequestScheduler(requestSchedulers[1]);
        TestDatabaseDescripter.getRequestScheduler().queue(Thread.currentThread(), "system", 100);
    }

    @Test
    public void test_starvation() throws TimeoutException, InterruptedException, ExecutionException
    {
        RequestSchedulerOptions options = new RequestSchedulerOptions();
        options.throttle_limit = 2;
        setup(options);
        TestDatabaseDescripter.setRequestSchedulerId(RequestSchedulerId.keyspace);
        TestDatabaseDescripter.setRequestScheduler(requestSchedulers[1]);

        final AtomicBoolean running = new AtomicBoolean(true);
        executor.submit(new Runnable()
        {
            public void run()
            {
                while (running.get())
                {
                    try
                    {
                        TestDatabaseDescripter.getRequestScheduler().queue(Thread.currentThread(), "system", 100);
                    }
                    catch (TimeoutException ex)
                    {
                        running.set(false);
                    }
                    finally
                    {
                        TestDatabaseDescripter.getRequestScheduler().release();
                    }
                }
            }
        });

        Thread.sleep(100);
        TestDatabaseDescripter.getRequestScheduler().queue(Thread.currentThread(), "some_keyspace", 100);

        executor.submit(new Runnable()
        {
            public void run()
            {
                try
                {
                    TestDatabaseDescripter.getRequestScheduler().queue(Thread.currentThread(), "some_keyspace", 10000);
                }
                catch (TimeoutException ex)
                {}
            }
        });
        Thread.sleep(100);

        Assert.assertTrue(
                ((RoundRobinScheduler)TestDatabaseDescripter.getRequestScheduler()).getTaskCount().hasQueuedThreads());

        Assert.assertEquals(
                1,
                ((RoundRobinScheduler)TestDatabaseDescripter.getRequestScheduler()).getTaskCount().getQueueLength());

        TestDatabaseDescripter.getRequestScheduler().release();
        Assert.assertTrue(running.get());
        running.set(false);
    }

    private void testImpl(String name) throws InterruptedException, ExecutionException
    {
        for (RequestSchedulerId requestSchedulerId : RequestSchedulerId.values())
        {
            TestDatabaseDescripter.setRequestSchedulerId(requestSchedulerId);

            for (int j = 0 ; j < requestSchedulers.length ; ++j)
            {
                TestDatabaseDescripter.setRequestScheduler(requestSchedulers[j]);
                List<Future<Boolean>> futures = new ArrayList<>();
                final AtomicLong totalTime = new AtomicLong();

                for (int l = 0 ; l < 100 ; ++l)
                {
                    final int l_ = l;
                    Callable<Boolean> next = new Callable<Boolean>()
                    {
                        public Boolean call() throws Exception
                        {
                            try
                            {
                                String id = "unknown";
                                if (0 != l_ % 3)
                                {
                                    ThriftClientState state = new ThriftClientState(new SocketAddress(){});
                                    state.setKeyspace("system");
                                    id = state.getSchedulingValue();
                                }

                                long start = System.nanoTime();
                                TestDatabaseDescripter.getRequestScheduler().queue(Thread.currentThread(), id, 100);
                                totalTime.addAndGet(System.nanoTime() - start);
                            }
                            finally
                            {
                                TestDatabaseDescripter.getRequestScheduler().release();
                            }
                            return true;
                        }
                    };
                    futures.add(executor.submit(next));
                }

                for (Future<Boolean> future : futures)
                {
                    Assert.assertTrue(future.get());
                }
                if (TestDatabaseDescripter.getRequestScheduler() instanceof RoundRobinScheduler)
                {
                    Assert.assertFalse(
                            ((RoundRobinScheduler)TestDatabaseDescripter.getRequestScheduler())
                                    .getTaskCount()
                                    .hasQueuedThreads());
                }

                System.out.println(
                        name + "." +TestDatabaseDescripter.getRequestScheduler().getClass().getSimpleName()
                        + "." + TestDatabaseDescripter.getRequestSchedulerId()
                        + ". average time in queue for each request "
                        + TimeUnit.NANOSECONDS.toMicros(totalTime.get() / 100) + "μs");
            }
        }
    }

    private static class TestDatabaseDescripter extends DatabaseDescriptor
    {
        protected static void setRequestScheduler(IRequestScheduler requestScheduler)
        {
            DatabaseDescriptor.setRequestScheduler(requestScheduler);
        }
        protected static void setRequestSchedulerOptions(RequestSchedulerOptions requestSchedulerOptions)
        {
            DatabaseDescriptor.setRequestSchedulerOptions(requestSchedulerOptions);
        }
        protected static void setRequestSchedulerId(RequestSchedulerId requestSchedulerId)
        {
            DatabaseDescriptor.setRequestSchedulerId(requestSchedulerId);
        }
    }
}
