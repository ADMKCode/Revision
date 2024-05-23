package co.com.bancolombia.routes;

import co.com.bancolombia.binstash.SerializatorHelper;
import co.com.bancolombia.binstash.SingleTierObjectCacheUseCase;
import co.com.bancolombia.binstash.adapter.memory.MemoryStash;
import co.com.bancolombia.binstash.model.api.ObjectCache;
import co.com.bancolombia.binstash.model.api.Stash;
import co.com.bancolombia.cypher.KmsServices;
import co.com.bancolombia.d2b.cache.FunctionalCacheOpsImpl;
import co.com.bancolombia.d2b.model.cache.FunctionalCacheOps;
import co.com.bancolombia.datamask.databind.mask.DataMask;
import co.com.bancolombia.datamask.databind.mask.JsonSerializer;
import co.com.bancolombia.datamask.databind.unmask.DataUnmasked;
import co.com.bancolombia.datamask.databind.unmask.JsonDeserializer;
import co.com.bancolombia.exceptions.TechnicalException;
import co.com.bancolombia.logging.technical.LoggerFactory;
import co.com.bancolombia.logging.technical.logger.TechLogger;
import co.com.bancolombia.router.configuredroute.ConfiguredRoute;
import co.com.bancolombia.router.configuredroute.gateway.Mapper;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import reactor.core.publisher.Mono;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.ArrayList;
import java.util.function.Function;

import static co.com.bancolombia.exceptions.messages.TechnicalErrorMessage.JACKSON_MAPPER_ERROR;
import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;

@Configuration
public class RoutesConfig {

    private static final TechLogger techLogger = LoggerFactory.getLog(RoutesProvider.class.getName());
    public static final String ERROR_READING_ROUTES_FROM_FILE = "Error reading routes from file," +
            " falling back to stringRoutes";
    public static final String ROUTE_LOADED = "ROUTE LOADED - ";
    public static final String EXPECTED_AN_ARRAY = "Expected an array";
    public static final String ERROR_PROCESSING_MAP_NODE = "Error processing Map Node content: {}";
    public static final String ERROR_PROCESSING_READ_NODE = "Error processing Read Node content: {}";
    public static final String CHANNEL = "channel";
    public static final String TRANSACTION = "transaction";


    private static final int SIZE = 999;
    @Value("${config-routes.string}")
    private String stringRoutes;
    @Value("${config-routes.file}")
    private String fileRoutes;

    private final Function<ConfiguredRoute,String> key =
            conf -> String.format("%s-%s", conf.getChannel(), conf.getTransaction());

    @Bean
    @Primary
    public ObjectMapper objectMapperBean(final KmsServices kms) {
        var module = new SimpleModule();
        module.addSerializer(DataMask.class, new JsonSerializer(DataMask.class, kms));
        module.addSerializer(DataUnmasked.class, new JsonDeserializer(DataUnmasked.class, kms));
        return new ObjectMapper()
                .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                .registerModule(module)
                .disable(FAIL_ON_UNKNOWN_PROPERTIES);
    }

    @Bean
    public FunctionalCacheOps<ConfiguredRoute> cacheForRoutes(Mapper mapper) {
        Stash memoryStash = new MemoryStash.Builder()
                .expireAfter(Integer.MAX_VALUE)
                .maxSize(SIZE)
                .build();
        ObjectCache<ConfiguredRoute> objectCache = new SingleTierObjectCacheUseCase<>(memoryStash,
                new SerializatorHelper<>(new ObjectMapper()));
        var cache = new FunctionalCacheOpsImpl<>(objectCache, ConfiguredRoute.class);
        routeInformationLoaded(mapper, cache);
        return cache;
    }

    @SuppressWarnings("findsecbugs:PATH_TRAVERSAL_IN")
    public void routeInformationLoaded(Mapper mapper, FunctionalCacheOps<ConfiguredRoute> cacheOps) {
        Mono.just(new File(fileRoutes))
                .filter(File::exists)
                .flatMap(file -> {
                    try {
                        var jsonContent = new String(Files.readAllBytes(file.toPath()));
                        return processJsonNodes(jsonContent, mapper);
                    } catch (TechnicalException | IOException exception) {
                        techLogger.info(ERROR_READING_ROUTES_FROM_FILE);
                        return processJsonNodes(stringRoutes, mapper);
                    }
                })
                .flatMapIterable(configuredRoutes -> configuredRoutes)
                .doOnNext(route -> techLogger.info(new StringBuilder(ROUTE_LOADED)
                        .append(route.getChannel()).append("-").append(route.getTransaction())))
                .flatMap(route -> cacheOps.saveInCache(key.apply(route), route))
                .then()
                .subscribe();
    }


    @SuppressWarnings("fb-contrib:EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS")
    public Mono<List<ConfiguredRoute>> processJsonNodes(String jsonContent, Mapper mapper) {
        List<ConfiguredRoute> validRoutes = new ArrayList<>();
        var objectMapper = new ObjectMapper();
        var jsonFactory = objectMapper.getFactory();
        try (JsonParser parser = jsonFactory.createParser(jsonContent)) {
            if (parser.nextToken() != JsonToken.START_ARRAY) {
                throw new IllegalStateException(EXPECTED_AN_ARRAY);
            }
            while (parser.nextToken() != JsonToken.END_ARRAY) {
                JsonNode node = readNode(parser, objectMapper);
                if (isValidNode(node)) {
                    ConfiguredRoute route = mapNode(node, mapper);
                    if (route != null) {
                        validRoutes.add(route);
                    }
                }
            }
        } catch (IOException exception) {
            throw new TechnicalException(exception, JACKSON_MAPPER_ERROR);
        }
        return Mono.just(validRoutes);
    }


    private JsonNode readNode(JsonParser parser, ObjectMapper objectMapper) {
        try {
            return objectMapper.readTree(parser);
        } catch (IOException exception) {
            techLogger.info(ERROR_PROCESSING_READ_NODE, exception.getMessage());
            return null;
        }
    }

    public ConfiguredRoute mapNode(JsonNode node, Mapper mapper) {
        if (node == null) {
            return null;
        }
        try {
            return mapper.readValues(node.toString(), ConfiguredRoute.class);
        } catch (TechnicalException exception) {
            techLogger.info(ERROR_PROCESSING_MAP_NODE, exception.getMessage());
            return null;
        }
    }


    public boolean isValidNode(JsonNode node) {
        return node != null && node.has(CHANNEL) && node.has(TRANSACTION);
    }

}