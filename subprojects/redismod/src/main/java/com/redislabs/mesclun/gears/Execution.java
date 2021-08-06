package com.redislabs.mesclun.gears;

import lombok.Data;

@Data
public class Execution {

    private String id;
    private String status;
    private long registered;
}
