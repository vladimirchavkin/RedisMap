package org.redis;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class RedisMapTest {

    private RedisMap redisMap;

    @BeforeEach
    void setUp() {
        redisMap = new RedisMap("localhost", 6379, "test:map");
        try (Jedis jedis = new Jedis("localhost", 6379)) {
            jedis.flushDB();
        }
    }

    @Test
    void testPutAndGet() {
        // Arrange
        redisMap.put("key1", "value1");

        // Assert
        assertEquals("value1", redisMap.get("key1"),
                "Значение должно быть равно value1");

        assertNull(redisMap.get("key2"),
                "Несуществующий ключ должен вернуть null");
    }

    @Test
    void testPutNullKeyOrValue() {
        // Assert
        assertThrows(NullPointerException.class, () -> redisMap.put(null, "value1"),
                "put с key = null");

        assertThrows(NullPointerException.class, () -> redisMap.put("key1", null),
                "put с value = null");
    }

    @Test
    void testRemove() {
        // Arrange
        redisMap.put("key1", "value1");

        // Assert
        assertEquals("value1", redisMap.remove("key1"),
                "Значение должно быть равно value1");

        assertNull(redisMap.get("key1"),
                "Удаленный ключ должен вернуть null");

        assertNull(redisMap.remove("key2"),
                "Несуществующий ключ должен вернуть null");
    }

    @Test
    void testContainsKey() {
        // Arrange
        redisMap.put("key1", "value1");

        // Assert
        assertTrue(redisMap.containsKey("key1"),
                "Существующий ключ должен вернуть true");

        assertFalse(redisMap.containsKey("key2"),
                "Несуществующий ключ должен вернуть false");

        assertFalse(redisMap.containsKey(new Object()),
                "Не строковый ключ должен вернуть false");
    }

    @Test
    void testContainsValue() {
        // Arrange
        redisMap.put("key1", "value1");

        // Assert
        assertTrue(redisMap.containsValue("value1"),
                "Существующее значение должно вернуть true");

        assertFalse(redisMap.containsValue("value2"),
                "Несуществующее значение должно вернуть false");

        assertFalse(redisMap.containsValue(new Object()),
                "Не строковое значение должно вернуть false");
    }

    @Test
    void testSizeAndIsEmpty() {
        // Arrange
        redisMap.put("key1", "value1");

        // Assert
        assertEquals(1, redisMap.size(),
                "Размер должен быть равен 1");

        assertFalse(redisMap.isEmpty(),
                "Карта не должна быть пустой");

        redisMap.clear();

        assertTrue(redisMap.isEmpty(),
                "Карта должна быть пустой");
    }

    @Test
    void testPutAll() {
        // Arrange
        Map<String, String> input = new HashMap<>();
        input.put("key1", "value1");
        input.put("key2", "value2");
        redisMap.putAll(input);

        // Assert
        assertEquals("value1", redisMap.get("key1"),
                "Значение должно быть равно value1");

        assertEquals("value2", redisMap.get("key2"),
                "Значение должно быть равно value2");

        assertEquals(2, redisMap.size(),
                "Размер должен быть равен 2");

        assertThrows(NullPointerException.class, () -> redisMap.putAll(null),
                "putAll с null");
    }

    @Test
    void testClear() {
        // Arrange
        redisMap.put("key1", "value1");
        redisMap.put("key2", "value2");

        // Act
        redisMap.clear();

        // Assert
        assertTrue(redisMap.isEmpty(),
                "Карта должна быть пустой");

        assertEquals(0, redisMap.size(),
                "Размер должен быть равен 0");

        assertNull(redisMap.get("key1"),
                "Удаленный ключ должен вернуть null");
    }

    @Test
    void testValues() {
        // Arrange
        redisMap.put("key1", "value1");
        redisMap.put("key2", "value2");

        // Act
        Collection<String> values = redisMap.values();

        // Assert
        assertEquals(2, values.size(),
                "values должен содержать 2 элемента");

        assertTrue(values.contains("value1"),
                "values должен содержать value1");

        assertTrue(values.contains("value2"),
                "values должен содержать value2");
    }

    @Test
    void testEntrySet() {
        // Arrange
        redisMap.put("key1", "value1");
        redisMap.put("key2", "value2");

        // Act
        Set<Map.Entry<String, String>> entrySet = redisMap.entrySet();

        Map<String, String> result = new HashMap<>();
        for (Map.Entry<String, String> entry : entrySet) {
            result.put(entry.getKey(), entry.getValue());
        }

        // Assert
        assertEquals(2, entrySet.size(),
                "entrySet должен содержать 2 элемента");

        assertEquals("value1", result.get("key1"),
                "entrySet должен содержать пару key1:value1");

        assertEquals("value2", result.get("key2"),
                "entrySet должен содержать пару key2:value2");
    }

    @Test
    void testRedisConnectionFailure() {
        // Arrange
        RedisMap badMap = new RedisMap("localhost", 9999, "test:map");

        // Assert
        assertThrows(JedisConnectionException.class, () -> badMap.put("key1", "value1"),
                "Redis недоступен");
    }
}