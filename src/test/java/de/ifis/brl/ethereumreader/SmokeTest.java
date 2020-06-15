package de.ifis.brl.ethereumreader;

import de.ifis.ethereumreader.EthereumTransactionExtractor;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class SmokeTest {

    @DisplayName("does the json-File contain a timestamp? Requires at least 1 transaction.")
    @Test
    @Disabled
    void testExtractingToJsonFile() throws IOException, InterruptedException {
        // set up client
        var url = "wss://websockets.bloxberg.org";
        var fileName = "test.json";
        var startBlock = 33;
        var endBlock = 34;

        var extractor = new EthereumTransactionExtractor(url, fileName);

        extractor.extractFromTo(startBlock, endBlock);

        Path outputfile = Path.of(fileName);
        List<String> lines = Files.readAllLines(outputfile);

        // check if there is a timestamp in the file
        assertTrue(lines.get(0).contains("hash"));
    }

    @Test
    void asReactiveWs() throws IOException, InterruptedException {
        // set up client
        var url = "wss://websockets.bloxberg.org";
        var fileName = "test.json";
        var startBlock = 33;
        var endBlock = 34;

        var extractor = new EthereumTransactionExtractor(url, fileName);

        long start = System.currentTimeMillis();
        extractor.extractFromTo(1, 1000);
        long duration = System.currentTimeMillis() - start;
        System.out.println(duration);
    }

    @Test
    void asReactiveHttp() throws IOException, InterruptedException {
        // set up client
        var url = "https://core.bloxberg.org";
        var fileName = "test.json";
        var startBlock = 33;
        var endBlock = 34;

        var extractor = new EthereumTransactionExtractor(url, fileName, true);

        long start = System.currentTimeMillis();
        extractor.extractFromTo(1, 1000);
        long duration = System.currentTimeMillis() - start;
        System.out.println(duration);
    }


    @Test
    void asConsumer() throws IOException, InterruptedException {
        // set up client
        var url = "wss://websockets.bloxberg.org";
        var fileName = "test.json";
        var startBlock = 33;
        var endBlock = 34;

        var extractor = new EthereumTransactionExtractor(url, fileName);

        long start = System.currentTimeMillis();
        extractor.extractFromToConsumer(1, 1000);
        long duration = System.currentTimeMillis() - start;
        System.out.println(duration);
    }

}





