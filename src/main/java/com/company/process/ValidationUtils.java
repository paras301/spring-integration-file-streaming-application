package com.company.process;

import com.company.vo.MappingConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.validator.GenericValidator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class ValidationUtils {
    private static final String INTEGER ="Integer";
    
    private static final String DOUBLE ="Double";
    
    private static final String DATE ="Date";
    
    
    
    private static boolean validateNumber(String value) {
        if(NumberUtils.isDigits(value)) {
            return true;
        }
        return false;
    }
    
    private static boolean validateDate(String value, String datePattern) {
        if(GenericValidator.isDate(value,datePattern, true)) {
            return true;
        }
        return false;
        
    }
    
    private static boolean validateDouble(String value) {
        if(NumberUtils.isCreatable(value) && !NumberUtils.isDigits(value)) {
            return true;
        }
        return false;
        
    }

    private static boolean validateMetadata(String dataType, String nullAllowed, String value, String datePattern) {
        if(dataType.equalsIgnoreCase(INTEGER)) {
            if(!validateNumber(value)) {
                log.error("ValidationUtils:metadata validation failed for field value {} and datatype {} ", value,dataType);
                return false;
            }
        }
        if(dataType.equalsIgnoreCase(DATE)) {
            if( !validateDate(value,datePattern)) {
                log.error("ValidationUtils:metadata validation failed for field value {} and datatype {} ", value,dataType);
                return false;
            }
        }
        if(dataType.equalsIgnoreCase(DOUBLE)) {
            if(!validateDouble(value)) {
                log.error("ValidationUtils:metadata validation failed for field value {} and datatype {} ", value,dataType);
                return false;
            }
        }
        if(nullAllowed.equalsIgnoreCase("n")) {
            if(StringUtils.isEmpty(value)) {
                log.error("ValidationUtils:not null validation failed for field value {} and datatype {} ", value,dataType);
                return false;
            }
        }
        return true;
    }

    public static List<String> dataValidation(List<Map<String, String>> csvData, MappingConfiguration configuration) throws Exception {
        log.debug("Inside Data Validation ... ");
        List<String> csvErrorList = new ArrayList<>();
        List<String> errorList = new ArrayList<>();

        for(Map<String, String> row : csvData) {
            for(Map.Entry<String, String> entry : row.entrySet()) {

                String csvColumnName = StringUtils.strip(entry.getKey());
                String csvColumnValue = entry.getValue();
                List<MappingConfiguration.Mapping> mappingList = configuration.getConfig().stream()
                        .filter(e -> e.getCsvheader().equals(csvColumnName))
                        .collect(Collectors.toList());

                if (mappingList.size() == 0) errorList.add("\nMapping not found for ==> " + csvColumnName);
                else {
                    MappingConfiguration.Mapping mapping = mappingList.get(0);

                    if (!validateMetadata(mapping.getDatatype(), mapping.getNullAllowed(), csvColumnValue, configuration.getDatepattern())) {
                        errorList.add("\nDatatype Validation failed for csv column ==> " + csvColumnName
                                + "... datatype -- " + mapping.getDatatype()
                                + "... csvColumnValue -- " + csvColumnValue
                                + "... datePattern -- " + configuration.getDatepattern());
                    }
                }
            }

            if(errorList.size() > 0) {
                errorList.add("\nCSV CONTENT --> " + row);
                errorList.add("\n===================================\n\n");

                csvErrorList.addAll(errorList.stream().distinct().collect(Collectors.toList()));
                errorList.clear();
            }

        }

        if(csvErrorList.size() > 0) {
            log.error("Error rows found inside CSV --> " + errorList);
        }

        return csvErrorList;
    }
}