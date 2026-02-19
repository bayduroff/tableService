package com.example.tableservice.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import groovy.lang.GroovyObject;
import groovy.xml.XmlSlurper;
import groovy.xml.slurpersupport.Attribute;
import groovy.xml.slurpersupport.GPathResult;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.xml.sax.XMLReader;

import javax.xml.parsers.SAXParserFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
public class XmlProcessorService {
    private static final String XML_URL = "https://expro.ru/bitrix/catalog_export/export_Sai.xml";
    private static final Logger log = LoggerFactory.getLogger(XmlProcessorService.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private GPathResult cachedXml;
    private Map<String, List<Map<String, String>>> xmlTables;
    private final JdbcTemplate jdbcTemplate;

    public XmlProcessorService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @PostConstruct
    public void init() throws Exception {
        parseXmlToMap(); // заполняем xmlTables при старте
        log.info("XML successfully parsed, tables: {}", xmlTables.keySet());
    }

    // Настройки парсинга XML
    private GPathResult parseXml() {
        if (cachedXml == null) {
            try {
                SAXParserFactory factory = SAXParserFactory.newInstance();
                factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", false);
                factory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
                factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
                factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
                factory.setFeature("http://xml.org/sax/features/validation", false);

                XMLReader reader = factory.newSAXParser().getXMLReader();
                XmlSlurper slurper = new XmlSlurper(reader);
                cachedXml = slurper.parse(XML_URL);
            } catch (Exception e) {
                throw new RuntimeException("Не удалось загрузить XML: " + e.getMessage(), e);
            }
        }
        return cachedXml;
    }

    // Получение узла <shop> (используется внутри)
    private GPathResult getShop() {
        GPathResult root = parseXml();
        GPathResult shop = (GPathResult) root.getProperty("shop");
        if (shop == null) {
            throw new RuntimeException("Элемент <shop> не найден в XML");
        }
        return shop;
    }

    // Основной метод парсинга: заполняет xmlTables
    private void parseXmlToMap() throws Exception {
        GPathResult shop = getShop();
        xmlTables = new LinkedHashMap<>();

        for (Object collectionObj : shop.children()) {
            GPathResult collection = (GPathResult) collectionObj;
            if (collection.children().size() == 0) continue;

            String collectionName = collection.name();
            List<Map<String, String>> records = new ArrayList<>();

            for (Object itemObj : collection.children()) {
                GPathResult item = (GPathResult) itemObj;
                Map<String, String> record = new LinkedHashMap<>();

                // Получаем атрибуты через GroovyObject
                GroovyObject groovyItem = (GroovyObject) item;
                Map attrMap = (Map) groovyItem.invokeMethod("attributes", null);
                if (attrMap != null) {
                    for (Object entryObj : attrMap.entrySet()) {
                        Map.Entry entry = (Map.Entry) entryObj;
                        String key = entry.getKey().toString();
                        String value = entry.getValue() != null ? entry.getValue().toString() : "";
                        record.put("@" + key, value);
                    }
                }

                // Сбор параметров для JSON (например, для offers)
                Map<String, String> params = new LinkedHashMap<>();

                // Обработка дочерних узлов (теги)
                for (Object childObj : item.children()) {
                    if (childObj instanceof Attribute) continue; // атрибуты уже обработаны

                    GPathResult child = (GPathResult) childObj;
                    String childName = child.name();

                    // Обработка <param>
                    if ("param".equals(childName)) {
                        String paramName = child.getProperty("@name").toString();
                        String paramValue = child.text();
                        if (paramName != null && !paramName.trim().isEmpty()) {
                            params.put(paramName, paramValue);
                        }
                        continue;
                    }

                    // Обычный тег без детей
                    if (child.children().size() == 0) {
                        String childText = child.text();
                        if (childText != null && !childText.trim().isEmpty()) {
                            record.put(childName, childText.trim());
                        }
                    }
                }

                // Добавляем параметры как JSON
                if (!params.isEmpty()) {
                    record.put("params_json", objectMapper.writeValueAsString(params));
                }

                // Текстовое содержимое самого элемента
                String text = item.text();
                if (text != null && !text.trim().isEmpty()) {
                    record.putIfAbsent("_text", text.trim());
                }

                records.add(record);
            }

            xmlTables.put(collectionName, records);
        }
    }

    // 1. Возвращает названия таблиц
    public List<String> getTableNames() {
        return new ArrayList<>(xmlTables.keySet());
    }

    // 2. Генерирует DDL для таблицы
    public String getTableDDL(String tableName) {
        if (!xmlTables.containsKey(tableName)) {
            return "-- Table '" + tableName + "' not found in XML.";
        }

        List<Map<String, String>> records = xmlTables.get(tableName);
        if (records.isEmpty()) {
            return "-- No data for table " + tableName + ", cannot determine columns.";
        }

        // Собираем все уникальные сырые ключи из всех записей
        Set<String> allRawColumns = records.stream()
                .flatMap(rec -> rec.keySet().stream())
                .collect(Collectors.toCollection(LinkedHashSet::new));

        // Преобразуем в имена колонок для SQL
        Set<String> columnNames = allRawColumns.stream()
                .map(this::toColumnName)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        StringBuilder ddl = new StringBuilder("CREATE TABLE IF NOT EXISTS " + tableName + " (\n");
        ddl.append("    id SERIAL PRIMARY KEY,\n");

        for (String colName : columnNames) {
            String dataType = "TEXT";
            if (tableName.equalsIgnoreCase("offers") && colName.equals(toColumnName("vendorCode"))) {
                dataType = "VARCHAR(255) UNIQUE";
            } else if (colName.equals("_id") && !tableName.equalsIgnoreCase("offers")) {
                dataType = "VARCHAR(255) UNIQUE";
            }
            ddl.append("    ").append(colName).append(" ").append(dataType).append(",\n");
        }

        ddl.setLength(ddl.length() - 2);
        ddl.append("\n);");
        return ddl.toString();
    }

    // 3. Обновление конкретной таблицы
    public void updateTable(String tableName) {
        if (!xmlTables.containsKey(tableName)) {
            throw new IllegalArgumentException("Table '" + tableName + "' not found in XML.");
        }
        List<Map<String, String>> records = xmlTables.get(tableName);
        if (records.isEmpty()) {
            log.info("Table {} has no records, skipping.", tableName);
            return;
        }

        // Собираем все возможные колонки из XML (с сохранением порядка)
        Set<String> allRawColumns = records.stream()
                .flatMap(rec -> rec.keySet().stream())
                .collect(Collectors.toCollection(LinkedHashSet::new));
        Set<String> xmlColumnNames = allRawColumns.stream()
                .map(this::toColumnName)
                .collect(Collectors.toCollection(LinkedHashSet::new)); // важно сохранить порядок!

        // Проверяем существование таблицы в БД
        Set<String> dbColumns = getTableColumns(tableName);

        if (dbColumns.isEmpty()) {
            // Таблицы нет – создаём через getTableDDL
            String ddl = getTableDDL(tableName);
            if (ddl.startsWith("--")) {
                throw new RuntimeException("Cannot create table: " + ddl);
            }
            jdbcTemplate.execute(ddl);
            dbColumns = getTableColumns(tableName);
            log.info("Table '{}' created successfully.", tableName);
        } else {
            // Проверка изменения структуры
            if (!dbColumns.containsAll(xmlColumnNames)) {
                Set<String> missing = new HashSet<>(xmlColumnNames);
                missing.removeAll(dbColumns);
                throw new IllegalStateException("Structure changed for table " + tableName +
                        ". New columns in XML: " + missing);
            }
        }

        // Определяем колонку для конфликта (UPSERT)
        String conflictColumn;
        if ("offers".equalsIgnoreCase(tableName)) {
            conflictColumn = toColumnName("vendorCode");
            if (!xmlColumnNames.contains(conflictColumn)) {
                throw new IllegalStateException("Table 'offers' must contain vendorCode column.");
            }
        } else {
            conflictColumn = "_id";
            if (!xmlColumnNames.contains(conflictColumn)) {
                throw new IllegalStateException("Table '" + tableName + "' must contain id attribute (_id).");
            }
        }

        // Маппинг для обратного поиска (имя колонки -> сырой ключ)
        Map<String, String> rawKeyByColumn = allRawColumns.stream()
                .collect(Collectors.toMap(this::toColumnName, Function.identity()));

        // Выполняем upsert для каждой записи
        for (Map<String, String> record : records) {
            upsertRecord(tableName, record, xmlColumnNames, rawKeyByColumn, conflictColumn);
        }

        log.info("Table '{}' updated successfully ({} records).", tableName, records.size());
    }

    // 4. Обновление всех таблиц
    public void updateAll() {
        for (String tableName : xmlTables.keySet()) {
            updateTable(tableName);
        }
    }

    // Преобразует "сырой" ключ (например, "@id") в допустимое имя колонки SQL
    private String toColumnName(String rawKey) {
        return rawKey.replace('@', '_').replace('.', '_').toLowerCase();
    }

    // Возвращает множество имён колонок существующей таблицы (или пустое, если таблицы нет)
    private Set<String> getTableColumns(String tableName) {
        try {
            String sql = "SELECT column_name FROM information_schema.columns WHERE table_name = ?";
            return new HashSet<>(jdbcTemplate.queryForList(sql, String.class, tableName));
        } catch (Exception e) {
            // Возвращаем пустое множество при ошибке (например, таблица не существует)
            return Collections.emptySet();
        }
    }

    // UPSERT одной записи
    private void upsertRecord(String tableName, Map<String, String> record,
                              Set<String> columnNames, Map<String, String> rawKeyByColumn,
                              String conflictColumn) {
        List<Object> values = new ArrayList<>();
        for (String colName : columnNames) {
            String rawKey = rawKeyByColumn.get(colName);
            values.add(record.get(rawKey));
        }

        String placeholders = String.join(",", Collections.nCopies(columnNames.size(), "?"));
        String updateSet = columnNames.stream()
                .map(col -> col + " = EXCLUDED." + col)
                .collect(Collectors.joining(", "));

        String insertSql = String.format(
                "INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s",
                tableName,
                String.join(",", columnNames),
                placeholders,
                conflictColumn,
                updateSet
        );

        jdbcTemplate.update(insertSql, values.toArray());
    }

    // Геттер таблиц для отладки
    public Map<String, List<Map<String, String>>> getXmlTables() {
        return xmlTables;
    }
}
