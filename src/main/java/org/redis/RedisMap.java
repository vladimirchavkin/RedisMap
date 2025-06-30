package org.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Реализация интерфейса Map<String, String>, использующая Redis в качестве хранилища.
 * Все ключи в Redis хранятся с префиксом для изоляции данных.
 */
public class RedisMap implements Map<String, String> {

    private final JedisPool jedisPool;
    private final String keyPrefix;

    /**
     * Конструктор для создания экземпляра RedisMap.
     *
     * @param host      хост Redis-сервера
     * @param port      порт Redis-сервера
     * @param keyPrefix префикс для ключей (может быть null, тогда используется пустой префикс)
     */
    public RedisMap(String host, int port, String keyPrefix) {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(128);
        config.setMaxIdle(128);
        config.setMinIdle(16);
        this.jedisPool = new JedisPool(config, host, port);
        this.keyPrefix = keyPrefix != null ? keyPrefix + ":" : "";
    }

    /**
     * Формирует полный ключ для Redis, добавляя префикс к переданному ключу.
     *
     * @param key ключ
     * @return полный ключ с префиксом
     */
    private String getFullKey(String key) {
        return keyPrefix + key;
    }

    /**
     * Возвращает количество ключей в Map.
     * <p>
     *
     * @return количество ключей
     * @throws redis.clients.jedis.exceptions.JedisException если Redis недоступен
     */
    @Override
    public int size() {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.keys(getFullKey("*")).size();
        }
    }

    /**
     * Проверяет, является ли Map пустой.
     *
     * @return true, если Map пуста, иначе false
     * @throws redis.clients.jedis.exceptions.JedisException если Redis недоступен
     */
    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    /**
     * Проверяет наличие ключа в Map.
     *
     * @param key ключ для проверки
     * @return true, если ключ существует, иначе false
     * @throws redis.clients.jedis.exceptions.JedisException если Redis недоступен
     */
    @Override
    public boolean containsKey(Object key) {
        if (!(key instanceof String)) return false;
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.exists(getFullKey((String) key));
        }
    }

    /**
     * Проверяет наличие значения в Map.
     * <p>
     *
     * @param value значение для проверки
     * @return true, если значение существует, иначе false
     * @throws redis.clients.jedis.exceptions.JedisException если Redis недоступен
     */
    @Override
    public boolean containsValue(Object value) {
        if (!(value instanceof String)) return false;
        try (Jedis jedis = jedisPool.getResource()) {
            Set<String> keys = jedis.keys(getFullKey("*"));
            for (String key : keys) {
                if (jedis.get(key).equals(value)) {
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * Возвращает значение, связанное с ключом.
     *
     * @param key ключ
     * @return значение, связанное с ключом, или null, если ключ отсутствует
     * @throws redis.clients.jedis.exceptions.JedisException если Redis недоступен
     */
    @Override
    public String get(Object key) {
        if (!(key instanceof String)) return null;
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.get(getFullKey((String) key));
        }
    }

    /**
     * Устанавливает значение для указанного ключа.
     *
     * @param key   ключ
     * @param value значение
     * @return предыдущее значение ключа или null, если ключа не было
     * @throws NullPointerException если ключ или значение null
     * @throws redis.clients.jedis.exceptions.JedisException если Redis недоступен
     */
    @Override
    public String put(String key, String value) {
        if (key == null || value == null) {
            throw new NullPointerException("Key or value cannot be null");
        }
        try (Jedis jedis = jedisPool.getResource()) {
            String fullKey = getFullKey(key);
            String oldValue = jedis.get(fullKey);
            jedis.set(fullKey, value);
            return oldValue;
        }
    }

    /**
     * Удаляет ключ и связанное с ним значение из Map.
     *
     * @param key ключ
     * @return удаленное значение или null, если ключ отсутствовал
     * @throws redis.clients.jedis.exceptions.JedisException если Redis недоступен
     */
    @Override
    public String remove(Object key) {
        if (!(key instanceof String)) return null;
        try (Jedis jedis = jedisPool.getResource()) {
            String fullKey = getFullKey((String) key);
            String value = jedis.get(fullKey);
            jedis.del(fullKey);
            return value;
        }
    }

    /**
     * Добавляет все пары ключ-значение из Map.
     *
     * @param m Map с парами ключ-значение
     * @throws NullPointerException если переданная Map или её элементы null
     * @throws redis.clients.jedis.exceptions.JedisException если Redis недоступен
     */
    @Override
    public void putAll(Map<? extends String, ? extends String> m) {
        if (m == null) {
            throw new NullPointerException("Map cannot be null");
        }
        try (Jedis jedis = jedisPool.getResource()) {
            for (Map.Entry<? extends String, ? extends String> entry : m.entrySet()) {
                if (entry.getKey() == null || entry.getValue() == null) {
                    throw new NullPointerException("Key or value in map cannot be null");
                }
                jedis.set(getFullKey(entry.getKey()), entry.getValue());
            }
        }
    }

    /**
     * Удаляет все ключи и значения из Map.
     * <p>
     *
     * @throws redis.clients.jedis.exceptions.JedisException если Redis недоступен
     */
    @Override
    public void clear() {
        try (Jedis jedis = jedisPool.getResource()) {
            Set<String> keys = jedis.keys(getFullKey("*"));
            jedis.del(keys.toArray(new String[0]));
        }
    }

    /**
     * Возвращает множество всех ключей в Map.
     * <p>
     *
     * @return множество ключей
     * @throws redis.clients.jedis.exceptions.JedisException если Redis недоступен
     */
    @Override
    public Set<String> keySet() {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.keys(getFullKey("*")).stream()
                    .map(key -> key.substring(keyPrefix.length()))
                    .collect(Collectors.toSet());
        }
    }

    /**
     * Возвращает коллекцию всех значений в Map.
     * <p>
     *
     * @return коллекция значений
     * @throws redis.clients.jedis.exceptions.JedisException если Redis недоступен
     */
    @Override
    public Collection<String> values() {
        try (Jedis jedis = jedisPool.getResource()) {
            Set<String> keys = jedis.keys(getFullKey("*"));
            List<String> values = new ArrayList<>();
            for (String key : keys) {
                values.add(jedis.get(key));
            }
            return values;
        }
    }

    /**
     * Возвращает множество всех пар ключ-значение в Map.
     * <p>
     *
     * @return множество пар ключ-значение
     * @throws redis.clients.jedis.exceptions.JedisException если Redis недоступен
     */
    @Override
    public Set<Entry<String, String>> entrySet() {
        try (Jedis jedis = jedisPool.getResource()) {
            Set<String> keys = jedis.keys(getFullKey("*"));
            Set<Entry<String, String>> entries = new HashSet<>();
            for (String key : keys) {
                String value = jedis.get(key);
                String shortKey = key.substring(keyPrefix.length());
                entries.add(Map.entry(shortKey, value));
            }
            return entries;
        }
    }
}