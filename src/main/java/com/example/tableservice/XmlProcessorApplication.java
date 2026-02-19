package com.example.tableservice;

import com.example.tableservice.service.XmlProcessorService;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

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
    public void run(String... args) throws Exception {
        try (Scanner scanner = new Scanner(System.in)) {
            System.out.println("\n=== XML Processor with Spring ===");
            System.out.println(xmlService.getTableNames());
            System.out.println("DDL for currencies:");
            System.out.println(xmlService.getTableDDL("currencies"));
            System.out.println("\nDDL for categories:");
            System.out.println(xmlService.getTableDDL("categories"));
            System.out.println("\nDDL for offers:");
            System.out.println(xmlService.getTableDDL("offers"));
            while (true) {
                /*System.out.println("\n=== XML Processor with Spring ===");
                System.out.println("1. Show table names");
                System.out.println("2. Show DDL for a table");
                System.out.println("3. Update all tables");
                System.out.println("4. Update specific table");
                System.out.println("5. Show column names");
                System.out.println("6. Check if column is ID");
                System.out.println("7. Exit");
                System.out.print("Choose: ");

                String choice = scanner.nextLine();
                try {
                    switch (choice) {
                        case "1":
                            System.out.println("Tables: " + processor.getTableNames());
                            break;
                        case "2":
                            System.out.print("Table name: ");
                            String t = scanner.nextLine();
                            System.out.println(processor.getTableDDL(t));
                            break;
                        case "3":
                            processor.updateAll();
                            System.out.println("All tables updated.");
                            break;
                        case "4":
                            System.out.print("Table name: ");
                            String t2 = scanner.nextLine();
                            processor.update(t2);
                            break;
                        case "5":
                            System.out.print("Table name: ");
                            String t3 = scanner.nextLine();
                            System.out.println("Columns: " + processor.getColumnNames(t3));
                            break;
                        case "6":
                            System.out.print("Table name: ");
                            String t4 = scanner.nextLine();
                            System.out.print("Column name: ");
                            String col = scanner.nextLine();
                            System.out.println("Is ID: " + processor.isColumnId(t4, col));
                            break;
                        case "7":
                            System.out.println("Bye!");
                            return;
                        default:
                            System.out.println("Invalid option.");
                    }
                } catch (Exception e) {
                    System.err.println("Error: " + e.getMessage());
                }*/
            }
        }
    }
}
