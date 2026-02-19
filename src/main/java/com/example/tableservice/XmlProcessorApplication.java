package com.example.tableservice;

import com.example.tableservice.service.XmlProcessorService;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.List;
import java.util.Scanner;

@SpringBootApplication
public class XmlProcessorApplication implements CommandLineRunner {

    private final XmlProcessorService xmlService;

    public XmlProcessorApplication(XmlProcessorService xmlService) {
        this.xmlService = xmlService;
    }

    public static void main(String[] args) {
        SpringApplication.run(XmlProcessorApplication.class, args);
    }

    @Override
    public void run(String... args) {
        try (Scanner scanner = new Scanner(System.in)) {
            System.out.println("=== XML Processor with Spring ===");

            //Раскомментировать для демонстрационной версии (без интерактива)
            /*
            // 1. Показать список таблиц
            List<String> tableNames = xmlService.getTableNames();
            System.out.println("Tables from XML: " + tableNames);

            // 2. Показать DDL для каждой таблицы
            for (String tableName : tableNames) {
                System.out.println("\nDDL for " + tableName + ":");
                System.out.println(xmlService.getTableDDL(tableName));
            }

            // 3. Обновить все таблицы
            System.out.println("\nUpdating all tables...");
            xmlService.updateAll();
            System.out.println("All tables updated successfully.");

            // 4. Завершение
            System.out.println("\nApplication finished. Exiting.");
            */

            //Закомментировать для демонстрационной версии (без интерактива)
            while (true) {
                System.out.println("\nВыберите действие:");
                System.out.println("1. Показать список таблиц (getTableNames)");
                System.out.println("2. Показать DDL для таблицы (getTableDDL)");
                System.out.println("3. Обновить все таблицы (updateAll)");
                System.out.println("4. Обновить конкретную таблицу (updateTable)");
                System.out.println("0. Выход");
                System.out.print("Ваш выбор: ");

                String choice = scanner.nextLine().trim();

                try {
                    switch (choice) {
                        case "1":
                            List<String> tables = xmlService.getTableNames();
                            System.out.println("Таблицы из XML: " + tables);
                            break;

                        case "2":
                            System.out.print("Введите имя таблицы: ");
                            String tableName = scanner.nextLine().trim();
                            String ddl = xmlService.getTableDDL(tableName);
                            System.out.println("DDL:\n" + ddl);
                            break;

                        case "3":
                            xmlService.updateAll();
                            System.out.println("Все таблицы успешно обновлены.");
                            break;

                        case "4":
                            System.out.print("Введите имя таблицы: ");
                            String tName = scanner.nextLine().trim();
                            xmlService.updateTable(tName);
                            System.out.println("Таблица '" + tName + "' обновлена.");
                            break;

                        case "0":
                            System.out.println("Программа завершена.");
                            return;

                        default:
                            System.out.println("Неверный ввод. Попробуйте снова.");
                    }
                } catch (Exception e) {
                    System.err.println("Ошибка: " + e.getMessage());
                }
            }
        }
    }
}
