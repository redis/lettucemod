package com.redis.lettucemod.test;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;

import com.redis.lettucemod.RedisModulesUtils;
import com.redis.lettucemod.api.sync.RedisGearsCommands;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.gears.Execution;
import com.redis.lettucemod.gears.ExecutionDetails;
import com.redis.lettucemod.gears.Registration;
import com.redis.lettucemod.output.ExecutionResults;
import com.redis.testcontainers.junit.jupiter.RedisTestContext;
import com.redis.testcontainers.junit.jupiter.RedisTestContextsSource;

public class GearsTests extends AbstractLettuceModTestBase {

	@ParameterizedTest
	@RedisTestContextsSource
	void pyExecute(RedisTestContext context) {
		RedisModulesCommands<String, String> sync = context.sync();
		sync.set("foo", "bar");
		ExecutionResults results = pyExecute(sync, "sleep.py");
		Assertions.assertEquals("1", results.getResults().get(0));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void pyExecuteUnblocking(RedisTestContext context) {
		RedisModulesCommands<String, String> sync = context.sync();
		sync.set("foo", "bar");
		String executionId = pyExecuteUnblocking(sync, "sleep.py");
		String[] array = executionId.split("-");
		Assertions.assertEquals(2, array.length);
		Assertions.assertEquals(40, array[0].length());
		Assertions.assertTrue(Integer.parseInt(array[1]) >= 0);
	}

//    @ParameterizedTest
//    @RedisTestContextsSource
//    void pyExecuteNoResults(RedisTestContext context) {
//        RedisModulesCommands<String, String> sync = context.sync();
//        ExecutionResults results = pyExecute(sync, "sleep.py");
//        Assertions.assertTrue(results.getResults().isEmpty());
//        Assertions.assertTrue(results.getErrors().isEmpty());
//    }

	private ExecutionResults pyExecute(RedisGearsCommands<String, String> sync, String resourceName) {
		return sync.pyexecute(load(resourceName));
	}

	private String pyExecuteUnblocking(RedisGearsCommands<String, String> sync, String resourceName) {
		return sync.pyexecuteUnblocking(load(resourceName));
	}

	private String load(String resourceName) {
		return RedisModulesUtils.toString(getClass().getClassLoader().getResourceAsStream(resourceName));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void dumpRegistrations(RedisTestContext context) {
		RedisModulesCommands<String, String> sync = context.sync();
		// Single registration
		List<Registration> registrations = sync.dumpregistrations();
		Assertions.assertEquals(0, registrations.size());
		ExecutionResults results = pyExecute(sync, "streamreader.py");
		Assertions.assertFalse(results.isError());
		registrations = sync.dumpregistrations();
		Assertions.assertEquals(1, registrations.size());
		Registration registration = registrations.get(0);
		Assertions.assertEquals("StreamReader", registration.getReader());
		Assertions.assertEquals("MyStreamReader", registration.getDescription());
		Assertions.assertEquals("async", registration.getData().getMode());
		Map<String, Object> args = registration.getData().getArgs();
		Assertions.assertEquals(3, args.size());
		Assertions.assertEquals(1L, args.get("batchSize"));
		Assertions.assertEquals("mystream", args.get("stream"));
		Assertions.assertEquals("OK", registration.getData().getStatus());
		Assertions.assertTrue(registration.getPrivateData().contains("'sessionId'"));

		// Multiple registrations
		sync.dumpregistrations().forEach(r -> sync.unregister(r.getId()));
		String function = "GB('KeysReader').register('*', keyTypes=['hash'])";
		Assertions.assertTrue(sync.pyexecute(function).isOk());
		Assertions.assertTrue(sync.pyexecute(function).isOk());
		registrations = sync.dumpregistrations();
		Assertions.assertEquals(2, registrations.size());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testGetResults(RedisTestContext context) {
		RedisModulesCommands<String, String> sync = context.sync();
		sync.set("foo", "bar");
		ExecutionResults results = sync.pyexecute("GB().foreach(lambda x: log('test')).register()");
		Assertions.assertTrue(results.isOk());
		Assertions.assertFalse(results.isError());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testDumpExecutions(RedisTestContext context) throws InterruptedException {
		RedisModulesCommands<String, String> sync = context.sync();
		List<Execution> executions = sync.dumpexecutions();
		executions.forEach(e -> sync.dropexecution(e.getId()));
		sync.set("foo", "bar");
		pyExecuteUnblocking(sync, "sleep.py");
		pyExecuteUnblocking(sync, "sleep.py");
		Thread.sleep(100);
		executions = sync.dumpexecutions();
		Assertions.assertEquals(2, executions.size());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testDropExecution(RedisTestContext context) throws InterruptedException {
		RedisModulesCommands<String, String> sync = context.sync();
		sync.set("foo", "bar");
		pyExecuteUnblocking(sync, "sleep.py");
		pyExecuteUnblocking(sync, "sleep.py");
		Thread.sleep(100);
		List<Execution> executions = sync.dumpexecutions();
		executions.forEach(e -> sync.abortexecution(e.getId()));
		executions.forEach(e -> sync.dropexecution(e.getId()));
		Assertions.assertEquals(0, sync.dumpexecutions().size());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void abortExecution(RedisTestContext context) throws InterruptedException {
		RedisModulesCommands<String, String> sync = context.sync();
		sync.set("foo", "bar");
		pyExecuteUnblocking(sync, "sleep.py");
		pyExecuteUnblocking(sync, "sleep.py");
		Thread.sleep(100);
		List<Execution> executions = sync.dumpexecutions();
		executions.forEach(e -> sync.abortexecution(e.getId()));
		for (Execution execution : executions) {
			ExecutionDetails details = sync.getexecution(execution.getId());
			Assertions.assertTrue(details.getPlan().getStatus().matches("done|aborted"));
		}
	}

}
