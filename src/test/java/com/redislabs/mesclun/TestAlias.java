package com.redislabs.mesclun;

import com.redislabs.mesclun.search.SearchResults;
import io.lettuce.core.RedisCommandExecutionException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@SuppressWarnings("ConstantConditions")
public class TestAlias extends AbstractSearchTest {

	private final static String ALIAS = "alias123";

	@BeforeEach
	public void initializeIndex() throws IOException {
		createBeerIndex();
	}

	@Test
	public void syncAdd() throws ExecutionException, InterruptedException {
		async.aliasAdd(ALIAS, INDEX).get();
		SearchResults<String, String> results = async.search(ALIAS, "*").get();
		assertTrue(results.size() > 0);
	}

	@Test
	public void asyncAdd() throws ExecutionException, InterruptedException {
		async.aliasAdd(ALIAS, INDEX).get();
		SearchResults<String, String> results = async.search(ALIAS, "*").get();
		assertTrue(results.size() > 0);
	}

	@Test
	public void reactiveAdd() {
		reactive.aliasAdd(ALIAS, INDEX).block();
		SearchResults<String, String> results = reactive.search(ALIAS, "*").block();
		assertTrue(results.size() > 0);
	}

	@Test
	public void syncDel() throws ExecutionException, InterruptedException {
		syncAdd();
		sync.aliasDel(ALIAS);
		try {
			sync.search(ALIAS, "*");
			fail("Alias was not removed");
		} catch (RedisCommandExecutionException e) {
			assertTrue(e.getMessage().contains("no such index") || e.getMessage().contains("Unknown Index name"));
		}
	}

	@Test
	public void asyncDel() throws ExecutionException, InterruptedException {
		asyncAdd();
		async.aliasDel(ALIAS).get();
		try {
			async.search(ALIAS, "*").get();
			fail("Alias was not removed");
		} catch (ExecutionException e) {
			assertTrue(e.getCause().getMessage().contains("no such index")
					|| e.getCause().getMessage().contains("Unknown Index name"));
		}
	}

	@Test
	public void reactiveDel() {
		reactiveAdd();
		reactive.aliasDel(ALIAS).block();
		try {
			reactive.search(ALIAS, "*").block();
			fail("Alias was not removed");
		} catch (RedisCommandExecutionException e) {
			assertTrue(e.getMessage().contains("no such index") || e.getMessage().contains("Unknown Index name"));
		}
	}

	@Test
	public void syncUpdate() throws ExecutionException, InterruptedException {
		syncAdd();
		String newAlias = "alias456";
		async.aliasUpdate(newAlias, INDEX).get();
		assertTrue(async.search(newAlias, "*").get().size() > 0);
	}

	@Test
	public void asyncUpdate() throws ExecutionException, InterruptedException {
		asyncAdd();
		String newAlias = "alias456";
		async.aliasUpdate(newAlias, INDEX);
		assertTrue(async.search(newAlias, "*").get().size() > 0);
	}

	@Test
	public void reactiveUpdate() {
		reactiveAdd();
		String newAlias = "alias456";
		reactive.aliasUpdate(newAlias, INDEX).block();
		SearchResults<String, String> results = reactive.search(newAlias, "*").block();
		Assertions.assertFalse(results.isEmpty());
	}

}
