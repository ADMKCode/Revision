package co.com.bancolombia.routes;

import co.com.bancolombia.cypher.KmsServices;
import co.com.bancolombia.d2b.model.cache.FunctionalCacheOps;
import co.com.bancolombia.exceptions.TechnicalException;
import co.com.bancolombia.logging.technical.logger.TechLogger;
import co.com.bancolombia.router.configuredroute.ConfiguredRoute;
import co.com.bancolombia.router.configuredroute.gateway.Mapper;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.function.Function;

import static co.com.bancolombia.exceptions.messages.TechnicalErrorMessage.JACKSON_MAPPER_ERROR;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class RoutesConfigTest {
    public static final String FILE_ROUTES = "fileRoutes";
    public static final String NONEXISTENT_FILE = "nonexistentFile";
    public static final String UNREADABLE_FILE = "unreadableFile";
    public static final String CONTENT = "content";
    public static final String FILE_WITH_SYNTAX_ERROR = "fileWithSyntaxError";
    public static final String CHANNEL_D2B_TRANSACTION_FILE = "[{\"channel\":\"D2B\",\"transaction\":}";
    public static final String CHANNEL_D2B_TRANSACTION_NUMBER_INVALID_TRUE =
            "[{\"channel\":\"D2B\",\"transaction\":\"9540\"}, {\"invalid\": true}]";
    public static final String INVALID_TRUE = "\"invalid\": true";
    public static final String INVALID_NODE = "Invalid node";
    public static final String D2B = "D2B";
    public static final String NUMBER = "9540";
    public static final String STRING_ROUTES = "stringRoutes";
    public static final String CHANNEL_D2B_TRANSACTION = "{\"channel\":\"D2B\",\"transaction\":\"9540\"}";
    public static final String CHANNEL = "channel";
    public static final String TRANSACTION = "transaction";
    public static final String CHANNEL_D2B_TRANSACTION_HEAD = "{channel:D2B,transaction:9540}";
    public static final String IS_VALID_NODE = "isValidNode";
    public static final String VALUE = "9540";
    public static final String RESULT = "9541";
    public static final String CHANNEL_D2B_TRANSACTION_NUMBER_DETAILS_KEY_VALUE = "[{\"channel\"" +
            ":\"D2B\",\"transaction\"" +
            ":\"9540\",\"details\":{\"key\":\"value\"}}]";
    public static final String CHANNEL_D2B_TRANSACTION_NUMBER_INVALID_TRUE_CHANNEL_D2B_TRANSACTION_NUMBER =
            "[{\"channel\":\"D2B\",\"transaction\":\"9540\"},{\"invalid\":\"true\"}," +
                    "{\"channel\":\"D2B\",\"transaction\"" + ":\"9541\"}]";
    @Mock
    private KmsServices kms;

    @Mock
    private Mapper mapper;

    @InjectMocks
    private RoutesConfig routesConfig;

    @Mock
    private FunctionalCacheOps<ConfiguredRoute> cacheOps;

    @Mock
    private TechLogger techLogger;

    private ObjectMapper objectMapper;

    @BeforeEach
    void init() {
        objectMapper = new ObjectMapper();
        routesConfig = new RoutesConfig();
        ReflectionTestUtils.setField(routesConfig, FILE_ROUTES, "file");
        ReflectionTestUtils.setField(routesConfig, STRING_ROUTES,
                "[{\"channel\":\"D2B\",\"transaction\":\"9540\"}]");
    }

    @Test
    void shouldGetObjectMapper() {
        assertNotNull(routesConfig.cacheForRoutes(mapper));
    }

    @Test
    void shouldCreateObjectMapperBean() {
        assertNotNull(routesConfig.objectMapperBean(kms));
    }

    @Test
    void shouldLoadRoutesFromFile() {
        assertDoesNotThrow(() -> routesConfig.routeInformationLoaded(mapper, cacheOps));
    }

    @Test
    void shouldFallBackToStringRoutesWhenFileNotFound() {
        ReflectionTestUtils.setField(routesConfig, FILE_ROUTES, NONEXISTENT_FILE);

        assertDoesNotThrow(() -> routesConfig.routeInformationLoaded(mapper, cacheOps));
    }

    @Test
    void shouldFallBackToStringRoutesWhenFileCannotBeRead() throws IOException {
        ReflectionTestUtils.setField(routesConfig, FILE_ROUTES, UNREADABLE_FILE);
        Path filePath = Paths.get(UNREADABLE_FILE);
        Files.write(filePath, CONTENT.getBytes());

        when(mapper.readValues(anyString(), any())).thenReturn(Mono.just(ConfiguredRoute.builder().build()));

        assertDoesNotThrow(() -> routesConfig.routeInformationLoaded(mapper, cacheOps));

        Files.deleteIfExists(filePath);
    }

    @Test
    void shouldHandleSyntaxErrorInFile() throws Exception {
        var filePath = Paths.get(FILE_WITH_SYNTAX_ERROR);
        Files.write(filePath, CHANNEL_D2B_TRANSACTION_FILE.getBytes());

        assertDoesNotThrow(() -> routesConfig.routeInformationLoaded(mapper, cacheOps));

        Files.deleteIfExists(filePath);
    }

    @Test
    void shouldSkipInvalidNodeAndContinueProcessing() {
        when(mapper.readValues(anyString(), any())).thenAnswer(invocation -> {
            var json = invocation.getArgument(0, String.class);
            if (json.contains(INVALID_TRUE)) {
                throw new RuntimeException(INVALID_NODE);
            }
            return ConfiguredRoute.builder().channel(D2B).transaction(NUMBER).build();
        });

        Mono<List<ConfiguredRoute>> result = routesConfig.processJsonNodes(CHANNEL_D2B_TRANSACTION_NUMBER_INVALID_TRUE, mapper);

        List<ConfiguredRoute> routes = result.block();

        assert routes != null;
        assertEquals(D2B, routes.get(0).getChannel());
        assertEquals(NUMBER, routes.get(0).getTransaction());
    }

    @Test
    void shouldReadNodeSuccessfully() throws Exception {
        var objectMapper = new ObjectMapper();
        var jsonFactory = objectMapper.getFactory();
        var parser = jsonFactory.createParser(CHANNEL_D2B_TRANSACTION);

        parser.nextToken();

        JsonNode node = ReflectionTestUtils.invokeMethod(routesConfig, "readNode", parser, objectMapper);
        assertNotNull(node);
        assertEquals(D2B, node.get(CHANNEL).asText());
        assertEquals(NUMBER, node.get(TRANSACTION).asText());
    }

    @Test
    void shouldMapNodeSuccessfully() {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode node = objectMapper.createObjectNode()
                .put(CHANNEL, D2B)
                .put(TRANSACTION, NUMBER);

        ConfiguredRoute configuredRoute = ConfiguredRoute.builder()
                .channel(D2B)
                .transaction(NUMBER)
                .build();

        when(mapper.readValues(anyString(), eq(ConfiguredRoute.class)))
                .thenReturn(configuredRoute);

        ConfiguredRoute result = ReflectionTestUtils.invokeMethod(routesConfig, "mapNode", node, mapper);
        assertNotNull(result);
        assertEquals(D2B, result.getChannel());
        assertEquals(NUMBER, result.getTransaction());
    }

    @Test
    void shouldReturnTrueForValidNode() {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode node = objectMapper.createObjectNode()
                .put(CHANNEL, D2B)
                .put(TRANSACTION, NUMBER);

        boolean isValid = Boolean.TRUE.equals(ReflectionTestUtils.invokeMethod(routesConfig, IS_VALID_NODE, node));
        assertTrue(isValid);
    }

    @Test
    void shouldReturnFalseForNodeMissingTransaction() {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode node = objectMapper.createObjectNode()
                .put(CHANNEL, D2B);

        boolean isValid = Boolean.TRUE.equals(ReflectionTestUtils.invokeMethod(routesConfig, IS_VALID_NODE, node));
        assertFalse(isValid);
    }

    @Test
    void shouldReturnFalseForNodeMissingChannel() {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode node = objectMapper.createObjectNode()
                .put(TRANSACTION, NUMBER);

        boolean isValid = Boolean.TRUE.equals(ReflectionTestUtils.invokeMethod(routesConfig, "isValidNode", node));
        assertFalse(isValid);
    }

    @Test
    void shouldHandleEmptyJsonFile() {
        ReflectionTestUtils.setField(routesConfig, FILE_ROUTES, "emptyFile");
        Path filePath = Paths.get("emptyFile");
        try {
            Files.write(filePath, new byte[0]);

            assertDoesNotThrow(() -> routesConfig.routeInformationLoaded(mapper, cacheOps));

            Files.deleteIfExists(filePath);
        } catch (IOException e) {
            fail("Exception should not have been thrown");
        }
    }

    @Test
    void shouldHandleMalformedJsonFile() {
        ReflectionTestUtils.setField(routesConfig, FILE_ROUTES, "malformedFile");
        Path filePath = Paths.get("malformedFile");
        try {
            Files.write(filePath, "{channel:D2B,transaction:9540".getBytes());

            assertDoesNotThrow(() -> routesConfig.routeInformationLoaded(mapper, cacheOps));

            Files.deleteIfExists(filePath);
        } catch (IOException e) {
            fail("Exception should not have been thrown");
        }
    }

    @Test
    void shouldHandleNestedJsonObjects() {
        var nestedJson = "[{\"channel\":\"D2B\",\"transaction\":\"9540\",\"details\":{\"key\":\"value\"}}]";

        when(mapper.readValues(anyString(), any())).thenAnswer(invocation -> {
            var json = invocation.getArgument(0, String.class);
            if (json.contains("details")) {
                return ConfiguredRoute.builder().channel(D2B).transaction(NUMBER).build();
            }
            return null;
        });

        Mono<List<ConfiguredRoute>> result = routesConfig.processJsonNodes(nestedJson, mapper);

        List<ConfiguredRoute> routes = result.block();

        assert routes != null;
        assertEquals(1, routes.size());
        assertEquals(D2B, routes.get(0).getChannel());
        assertEquals(NUMBER, routes.get(0).getTransaction());
    }

    @Test
    void shouldProcessMultipleValidAndInvalidNodes() {
        when(mapper.readValues(anyString(), any())).thenAnswer(invocation -> {
            var json = invocation.getArgument(0, String.class);
            if (json.contains(INVALID_TRUE)) {
                throw new RuntimeException(INVALID_NODE);
            }
            return ConfiguredRoute.builder().channel(D2B).transaction(json.contains(VALUE) ? VALUE : RESULT).build();
        });

        Mono<List<ConfiguredRoute>> result = routesConfig.processJsonNodes(
                CHANNEL_D2B_TRANSACTION_NUMBER_INVALID_TRUE_CHANNEL_D2B_TRANSACTION_NUMBER, mapper);

        List<ConfiguredRoute> routes = result.block();

        assert routes != null;
        assertEquals(2, routes.size());
        assertEquals("9540", routes.get(0).getTransaction());
        assertEquals("9541", routes.get(1).getTransaction());
    }

    @Test
    void shouldReturnNullWhenInvalidMap() {

        when(mapper.readValues(anyString(), any())).thenThrow(new TechnicalException(JACKSON_MAPPER_ERROR));

        Mono<List<ConfiguredRoute>> result = routesConfig.processJsonNodes(CHANNEL_D2B_TRANSACTION_NUMBER_DETAILS_KEY_VALUE, mapper);

        List<ConfiguredRoute> routes = result.block();

        assert routes != null;
        assertEquals(0, routes.size());
    }

    @Test
    void shouldReturnFalseForNullNode() {
        boolean isValid = Boolean.TRUE.equals(ReflectionTestUtils.invokeMethod(routesConfig,
                "isValidNode", (JsonNode) null));
        assertFalse(isValid);
    }

    @Test
    void shouldReturnFalseForEmptyNode() {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode node = objectMapper.createObjectNode();

        boolean isValid = Boolean.TRUE.equals(ReflectionTestUtils.invokeMethod(routesConfig, "isValidNode", node));
        assertFalse(isValid);
    }

    @Test
    void shouldReturnConfiguredRouteForValidNode() {
        JsonNode node = objectMapper.createObjectNode()
                .put("channel", "D2B")
                .put("transaction", "9540");

        ConfiguredRoute configuredRoute = ConfiguredRoute.builder()
                .channel("D2B")
                .transaction("9540")
                .build();

        when(mapper.readValues(anyString(), eq(ConfiguredRoute.class)))
                .thenReturn(configuredRoute);

        ConfiguredRoute result = routesConfig.mapNode(node, mapper);
        assertNotNull(result);
        assertEquals("D2B", result.getChannel());
        assertEquals("9540", result.getTransaction());
    }

    @Test
    void shouldReturnNullForNullNode() {
        ConfiguredRoute result = routesConfig.mapNode(null, mapper);
        assertNull(result);
    }

    @Test
    void shouldProcessJsonNodesSuccessfully() {
        var jsonContent = "[{\"channel\":\"D2B\",\"transaction\":\"9540\"}]";
        ConfiguredRoute configuredRoute = ConfiguredRoute.builder()
                .channel("D2B")
                .transaction("9540")
                .build();

        when(mapper.readValues(anyString(), eq(ConfiguredRoute.class)))
                .thenReturn(configuredRoute);

        Mono<List<ConfiguredRoute>> result = routesConfig.processJsonNodes(jsonContent, mapper);

        List<ConfiguredRoute> routes = result.block();

        assertNotNull(routes);
        assertEquals(1, routes.size());
        assertEquals("D2B", routes.get(0).getChannel());
        assertEquals("9540", routes.get(0).getTransaction());
    }

    @Test
    void shouldSkipInvalidNodes() {
        var jsonContent = "[{\"invalid\":true},{\"channel\":\"D2B\",\"transaction\":\"9540\"}]";
        ConfiguredRoute configuredRoute = ConfiguredRoute.builder()
                .channel("D2B")
                .transaction("9540")
                .build();

        when(mapper.readValues(anyString(), eq(ConfiguredRoute.class)))
                .thenReturn(configuredRoute);

        Mono<List<ConfiguredRoute>> result = routesConfig.processJsonNodes(jsonContent, mapper);

        List<ConfiguredRoute> routes = result.block();

        assertNotNull(routes);
        assertEquals(1, routes.size());
        assertEquals("D2B", routes.get(0).getChannel());
        assertEquals("9540", routes.get(0).getTransaction());
    }


    private void processJson(String json, Mapper mapper) {
        routesConfig.processJsonNodes(json, mapper).block();
    }

    @Test
    void shouldProcessEmptyJsonArray() {
        var emptyJsonArray = "[]";

        Mono<List<ConfiguredRoute>> result = routesConfig.processJsonNodes(emptyJsonArray, mapper);

        List<ConfiguredRoute> routes = result.block();

        assertNotNull(routes);
        assertTrue(routes.isEmpty());
    }

    @Test
    void shouldThrowExceptionForNonArrayJson() {
        var nonArrayJson = "{\"channel\":\"D2B\",\"transaction\":\"9540\"}";

        Exception exception = assertThrows(IllegalStateException.class, () -> processJson(nonArrayJson, mapper));

        assertEquals(RoutesConfig.EXPECTED_AN_ARRAY, exception.getMessage());
    }

    @Test
    void shouldGenerateCacheKeyForConfiguredRoute() throws Exception {
        ConfiguredRoute route = ConfiguredRoute.builder()
                .channel("D2B")
                .transaction("9540")
                .build();

        Field keyField = RoutesConfig.class.getDeclaredField("key");
        keyField.setAccessible(true);
        Function<ConfiguredRoute, String> key = (Function<ConfiguredRoute, String>) keyField.get(routesConfig);

        var cacheKey = key.apply(route);
        assertEquals("D2B-9540", cacheKey);
    }

    @Test
    void shouldThrowIllegalStateExceptionForMalformedJson() {
        var malformedJson = "{\"channel\":\"D2B\",\"transaction\":\"9540\"";
        Exception exception = assertThrows(IllegalStateException.class, () -> processJson(malformedJson, mapper));
        assertTrue(exception.getMessage().contains("Expected an array"));
    }

    @Test
    void shouldProcessValidJson() {
        var validJson = "[{\"channel\":\"D2B\",\"transaction\":\"9540\"}]";
        when(mapper.readValues(anyString(), any())).thenReturn(ConfiguredRoute.builder().channel("D2B").transaction("9540").build());

        Mono<List<ConfiguredRoute>> result = routesConfig.processJsonNodes(validJson, mapper);
        List<ConfiguredRoute> routes = result.block();

        assertNotNull(routes);
        assertDoesNotThrow(() -> routesConfig.processJsonNodes(validJson, mapper).block());
    }
}
