package com.largecsv.process.route;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.dataformat.csv.CsvDataFormat;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.QuoteMode;
import org.springframework.stereotype.Component;

//@Component
public class SedaProcessRoute extends RouteBuilder {

    @Override
    public void configure() throws Exception {
        CsvDataFormat csvParser = new CsvDataFormat(CSVFormat.DEFAULT);
        csvParser.setSkipHeaderRecord(true);
        csvParser.setQuoteMode(QuoteMode.ALL);

        from("{{inboxPath}}").id("customerDataRoute")
                .onCompletion().log("Customer data  processing finished").end()
                .log("Processing customer data ${file:name}")
                .split(body().tokenize("\n")).streaming() //no more parallel processing
                .choice()
                .when(simple("${body} contains 'HEADER TEXT'")) //strip out the header if it exists
                .log("Skipping first line")
                .endChoice()
                .otherwise()
                .to("seda:processCustomer?size=40&concurrentConsumers=20&blockWhenFull=true")
                .endChoice()
                ;
        
        from("seda:processCustomer?size=40&concurrentConsumers=20&blockWhenFull=true")
                .unmarshal(csvParser)
                .split(body())
                .to("{{outboxPath}}")
                ;
    }
}
