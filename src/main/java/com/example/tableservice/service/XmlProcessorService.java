package com.example.tableservice.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import groovy.xml.XmlSlurper;
import groovy.xml.slurpersupport.Attribute;
import groovy.xml.slurpersupport.GPathResult;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Service;
import org.xml.sax.XMLReader;

import javax.xml.parsers.SAXParserFactory;
import java.lang.reflect.Method;
import java.net.URL;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Service
public class XmlProcessorService {
    private static final String XML_URL = "https://expro.ru/bitrix/catalog_export/export_Sai.xml";

    private GPathResult cachedXml;          // кэшированный корень XML
    private Map<String, List<Map<String, String>>> xmlTables; // структура таблиц
    private final ObjectMapper objectMapper = new ObjectMapper();

    @PostConstruct
    public void init() throws Exception {
        parseXmlToMap(); // заполняем xmlTables при старте
    }

    // Парсинг XML с защитой от XXE и кэшированием
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

                // 1. Атрибуты (рефлексия)
                try {
                    Method attrMethod = item.getClass().getMethod("attributes");
                    Object attrObj = attrMethod.invoke(item);
                    if (attrObj instanceof Map) {
                        Map<?, ?> attrMap = (Map<?, ?>) attrObj;
                        for (Map.Entry<?, ?> entry : attrMap.entrySet()) {
                            String key = entry.getKey().toString();
                            String value = entry.getValue() != null ? entry.getValue().toString() : "";
                            record.put("@" + key, value);
                        }
                    }
                } catch (Exception e) {
                    // игнорируем
                }

                // Сбор параметров (только для offers, но можно для всех)
                Map<String, String> params = new LinkedHashMap<>();

                // 2. Дочерние узлы
                for (Object childObj : item.children()) {
                    if (childObj instanceof Attribute) {
                        Attribute attr = (Attribute) childObj;
                        record.putIfAbsent("@" + attr.name(), attr.text());
                        continue;
                    }

                    GPathResult child = (GPathResult) childObj;
                    String childName = child.name();

                    // Обработка <param>
                    if ("param".equals(childName)) {
                        // Получаем атрибут name
                        String paramName = child.getProperty("@name").toString();
                        String paramValue = child.text();
                        params.put(paramName, paramValue);
                        continue;
                    }

                    // Обычный тег без детей
                    if (child.children().size() == 0) {
                        String childText = child.text();
                        if (childText != null && !childText.trim().isEmpty()) {
                            record.put(childName, childText.trim());
                        }
                    }
                    // Если есть дети - игнорируем (можно добавить рекурсию при необходимости)
                }

                // Добавляем параметры как JSON
                if (!params.isEmpty()) {
                    String paramsJson = objectMapper.writeValueAsString(params);
                    record.put("params_json", paramsJson);
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

        // Собираем все ключи из всех записей
        Set<String> allColumns = new LinkedHashSet<>();
        for (Map<String, String> record : records) {
            allColumns.addAll(record.keySet());
        }

        StringBuilder ddl = new StringBuilder("CREATE TABLE IF NOT EXISTS " + tableName + " (\n");
        ddl.append("    id SERIAL PRIMARY KEY,\n");

        for (String col : allColumns) {
            String columnName = col.replace('@', '_').replace('.', '_');
            String dataType = "TEXT";
            boolean isVendorCode = col.toLowerCase().contains("vendorcode") ||
                    col.toLowerCase().contains("vendor_code");
            if (tableName.equalsIgnoreCase("offers") && isVendorCode) {
                dataType = "VARCHAR(255) UNIQUE";
            }
            ddl.append("    ").append(columnName).append(" ").append(dataType).append(",\n");
        }

        ddl.setLength(ddl.length() - 2);
        ddl.append("\n);");
        return ddl.toString();
    }

    // Геттер для отладки
    public Map<String, List<Map<String, String>>> getXmlTables() {
        return xmlTables;
    }
}
