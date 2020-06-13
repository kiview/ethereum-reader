package de.ifis.brl.ethereumread;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.DefaultBlockParameterNumber;
import org.web3j.protocol.websocket.WebSocketService;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class SmokeTest {

    @DisplayName("does the json-File contain a timestamp? Requires at least 1 transaction.")
    @Test
    void testExtractingToJsonFile() throws IOException {
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

    public static class EthereumTransactionExtractor implements AutoCloseable {


        private final WebSocketService webSocketService;
        private final Web3j web3j;
        private final ObjectMapper mapper = new ObjectMapper();
        private final JsonGenerator jsonGenerator;

        public EthereumTransactionExtractor(String url, String fileName) throws IOException {
            webSocketService = new WebSocketService(url, false);
            webSocketService.connect();
            web3j = Web3j.build(webSocketService);
            jsonGenerator = new JsonFactory()
                    .createGenerator(new File(fileName), JsonEncoding.UTF8)
                    .setCodec(new ObjectMapper());
        }

        public void extractFromTo(int startBlock, int endBlock) throws IOException {
            jsonGenerator.writeStartArray();
            web3j.replayPastTransactionsFlowable(
                    new DefaultBlockParameterNumber(startBlock),
                    new DefaultBlockParameterNumber(endBlock)
            ).doOnError(Throwable::printStackTrace)
                    .blockingSubscribe(jsonGenerator::writeObject, Throwable::printStackTrace);
            jsonGenerator.writeEndArray();
            jsonGenerator.close();
        }

        @Override
        public void close() {
            webSocketService.close();
        }
    }

}





