package com.redislabs.mesclun;

import com.github.dockerjava.zerodep.shaded.org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import com.redislabs.testcontainers.support.enterprise.DatabaseProvisioner;
import com.redislabs.testcontainers.support.enterprise.RestAPI;
import com.redislabs.testcontainers.support.enterprise.rest.Database;

public class ProvisionJRX {

    public static void main(String[] args) throws Exception {
        String password = System.getenv("JRX_PASSWORD");
        RestAPI restAPI = RestAPI.credentials(new UsernamePasswordCredentials("julien@redislabs.com", password.toCharArray())).host("jrx.demo.redislabs.com").build();
        DatabaseProvisioner provisioner = DatabaseProvisioner.restAPI(restAPI).build();
        Database db = Database.name("sparseclustermod").port(12000).ossCluster(true).shardPlacement(Database.ShardPlacement.SPARSE).modules(Database.Module.SEARCH, Database.Module.GEARS, Database.Module.TIMESERIES).build();
        provisioner.create(db);
    }
}
