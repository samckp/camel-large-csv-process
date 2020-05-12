package com.largecsv.process.route;

import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.dataformat.csv.CsvDataFormat;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.QuoteMode;
import org.springframework.stereotype.Component;

@Component
public class CSVProcessRoute extends RouteBuilder {

    @Override
    public void configure() throws Exception {

        CsvDataFormat csvParser = new CsvDataFormat(CSVFormat.DEFAULT);
        csvParser.setSkipHeaderRecord(false);
//        csvParser.setQuoteMode(QuoteMode.ALL);

        from("{{inboxPath}}")
                .routeId("LargeCSV-Process-Route")
                .log(LoggingLevel.INFO, "LargeCSV-Process-Route Started !!")
                .unmarshal(csvParser)
                .split(body())
                .streaming().parallelProcessing()
                .aggregate(constant(true), new ArrayListAggregationStrategy())
                .completionSize(100000) //"{{lineCount}}")
                .completionTimeout(10000)
//                .log(LoggingLevel.INFO, "${body}")
                .marshal(csvParser)
                .to("{{outboxPath}}")
                ;
    }
}


