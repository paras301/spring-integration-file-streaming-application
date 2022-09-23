package com.company.process;

import com.company.Config;
import com.company.vo.MappingConfiguration;
import com.fasterxml.jackson.dataformat.csv.CsvGenerator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.annotation.InboundChannelAdapter;
import org.springframework.integration.annotation.Poller;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.file.FileHeaders;
import org.springframework.integration.file.FileReadingMessageSource;
import org.springframework.integration.file.filters.SimplePatternFileListFilter;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class ProcessorCSV {
    @Autowired
    Config config;

    @Autowired
    @Qualifier("orderMap")
    MappingConfiguration orderMapping;

    @Bean
    public MessageChannel fileChannel() {
        return new DirectChannel();
    }

    @Bean
    @InboundChannelAdapter(value = "csvChannel", poller = @Poller(fixedDelay = "10000"))
    public MessageSource<File> csvFileReadingMessageSource() {
        FileReadingMessageSource sourceReader= new FileReadingMessageSource();
        sourceReader.setDirectory(new File(config.getCsv_input_dir()));
        sourceReader.setFilter(new SimplePatternFileListFilter(config.getCsv_file_pattern()));
        return sourceReader;
    }

    @ServiceActivator(inputChannel= "csvChannel")
    public void processCsv(Message<?> message) throws Exception {
        File filePath = message.getHeaders().get(FileHeaders.ORIGINAL_FILE, File.class);
        String fileName = message.getHeaders().get(FileHeaders.FILENAME, String.class);
        log.info("CSV File found ==> " + filePath.getPath());

        move_files(filePath.getPath(), config.getCsv_processing_dir() + fileName);

        try {
            String fileContent = Files.readString(Paths.get(config.getCsv_processing_dir() + fileName));
            List<Map<String, String>> data = convertCSVtoMap(fileContent);


            //VALIDATE CSV DATA FIRST
            List<String> csvErrorList = ValidationUtils.dataValidation(data, orderMapping);

            //IF NO ERRORS
            if(csvErrorList.size() == 0) {
                List<Map<String, String>> data_out = calculateDiscount(data, orderMapping);
                writeCSV(data_out, config.getCsv_output_dir() + fileName);
                move_files(config.getCsv_processing_dir() + fileName, config.getCsv_success_dir() + fileName);

            } else {
                writeFile(csvErrorList, config.getCsv_error_dir() + fileName + ".log");
                throw new Exception("Data validation failed for CSV, Error files written at: " + config.getCsv_error_dir() + fileName + ".log");
            }
        } catch (Exception e) {
            log.error("Exception while processing the file", e);
            move_files(config.getCsv_processing_dir() + fileName, config.getCsv_error_dir() + fileName);
        }
    }

    private List<Map<String, String>> calculateDiscount(List<Map<String, String>> data, MappingConfiguration orderMapping) {
        for(Map<String, String> row : data) {
            Double unit_price = Double.parseDouble(row.getOrDefault("unit_price", "0.0"));
            Integer units = Integer.parseInt(row.getOrDefault("units", "0"));
            Double discount = Double.parseDouble(row.getOrDefault("discount", "0.0"));

            Double temp_price = unit_price * units;
            Double final_price = temp_price - ((discount / 100) * temp_price);
            row.put("final_price", final_price.toString());
        }
        return data;
    }

    public void move_files(String src, String tgt) throws IOException  {
        log.info("Moving files from --> " + src + " --> to --> " + tgt);
        if(!Files.exists(Paths.get(tgt).getParent()))
            Files.createDirectory(Paths.get(tgt).getParent());

        Files.move(Paths.get(src), Paths.get(tgt), StandardCopyOption.REPLACE_EXISTING);
        log.info("Moved!");
    }

    public void writeCSV(List<Map<String, String>> data, String path) throws Exception {
        log.info("Writing CSV to local at --> " + path);
        if(!Files.exists(Paths.get(path).getParent()))
            Files.createDirectory(Paths.get(path).getParent());

        FileWriter writer = new FileWriter(path);

        CsvSchema.Builder schemaBuilder = CsvSchema.builder();
        for (String col : data.get(0).keySet()) {
            schemaBuilder.addColumn(col);
        }

        CsvSchema schema = schemaBuilder.build().withHeader();

        CsvMapper mapper = new CsvMapper();
        mapper.configure(CsvGenerator.Feature.ALWAYS_QUOTE_STRINGS, true);
        mapper.writer(schema).writeValues(writer).writeAll(data);
        writer.flush();
        writer.close();

        log.info("Written!");
    }

    public void writeFile(List<String> data, String path) throws Exception {
        log.info("Writing File to local at --> " + path);
        if(!Files.exists(Paths.get(path).getParent()))
            Files.createDirectory(Paths.get(path).getParent());

        FileWriter writer = new FileWriter(path);

        for(String d : data)
            writer.write(d + System.lineSeparator());

        writer.flush();
        writer.close();

        log.info("Written!");
    }

    public List<Map<String, String>> convertCSVtoMap(String fileContent) throws Exception {
        List<Map<String, String>> data = new ArrayList<>();

        CsvMapper csvMapper = new CsvMapper();
        csvMapper.enable(CsvParser.Feature.SKIP_EMPTY_LINES);

        Iterator<Map<String, String>> iterator = csvMapper
                .readerFor(Map.class)
                .with(CsvSchema.emptySchema().withHeader())
                .readValues(new StringReader(fileContent));

        while (iterator.hasNext())
            data.add(iterator.next());

        return data;
    }

}
