package de.ifis.ethereumreader;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.DefaultBlockParameterNumber;
import org.web3j.protocol.http.HttpService;
import org.web3j.protocol.websocket.WebSocketService;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class EthereumTransactionExtractor implements AutoCloseable {


    private WebSocketService webSocketService;
    private final Web3j web3j;
    private final ObjectMapper mapper = new ObjectMapper();
    private final JsonGenerator jsonGenerator;

    public EthereumTransactionExtractor(String url, String fileName) throws IOException {
        webSocketService = new WebSocketService(url, false);
        webSocketService.connect();
        web3j = Web3j.build(webSocketService);
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        jsonGenerator = new JsonFactory()
                .createGenerator(new File(fileName), JsonEncoding.UTF8)
                .setCodec(mapper);
    }

    public EthereumTransactionExtractor(String url, String fileName, boolean b) throws IOException {
        web3j = Web3j.build(new HttpService(url));
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        jsonGenerator = new JsonFactory()
                .createGenerator(new File(fileName), JsonEncoding.UTF8)
                .setCodec(mapper);
    }

    public void extractFromTo(int startBlock, int endBlock) throws IOException, InterruptedException {

//        jsonGenerator.writeStartArray();
        AtomicInteger i = new AtomicInteger(0);
        web3j.replayPastBlocksFlowable(
                new DefaultBlockParameterNumber(startBlock),
                new DefaultBlockParameterNumber(endBlock), false
        )
                .doOnError(Throwable::printStackTrace)
                .blockingSubscribe(bloc -> {
                    System.out.println(Thread.currentThread().getName());
                    System.out.println(i.incrementAndGet());
                    //
//                    bloc.getBlock().setNonce("0x0");
//                    jsonGenerator.writeObject(bloc);
                });

//        jsonGenerator.writeEndArray();
//        jsonGenerator.close();
    }

    public void extractFromToConsumer(int startBlock, int endBlock) throws IOException, InterruptedException {
        AtomicInteger k = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(endBlock);
        webSocketService.connect(s -> {
            System.out.println(Thread.currentThread().getName());
            System.out.println(k.incrementAndGet());
            System.out.println(s);
            latch.countDown();
        }, throwable -> {}, () -> {});
        for (int i = startBlock; i <= endBlock; i++) {
            web3j.ethGetBlockByNumber(new DefaultBlockParameterNumber(i), false).sendAsync();
        }
        latch.await();
    }

    @Override
    public void close() {
        webSocketService.close();
    }
}
