package com.company.vo;

import lombok.Data;

import java.util.List;

@Data
public class MappingConfiguration {
    private List<Mapping> config;
    private String datepattern;

    @Data
    public static class Mapping {
        private String csvheader;
        private String datatype;
        private String nullAllowed;
    }
}
