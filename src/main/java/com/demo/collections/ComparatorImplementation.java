package com.demo.collections;

import com.demo.model.Employee;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class ComparatorImplementation {
    public static void main(String[] args) {

        Comparator<Employee> compareById = (Employee employee1, Employee employee2) -> {
            if(employee1.getId() > employee2.getId())
                return 1;
            else
                return -1;
        };

        Comparator<Employee> compareByNameLength = (employee1, employee2) -> employee1.getName().length() > employee2.getName().length() ? 1 : -1;

        List<Employee> employees = new ArrayList<>();
        employees.add(new Employee(4, "Saaquib", "Khan"));
        employees.add(new Employee(2, "Shekhu", ""));
        employees.add(new Employee(1, "Shaktiimaannn", ""));
        employees.add(new Employee(6, "SkRuddy", ""));
        employees.add(new Employee(9, "", ""));


        System.out.println("Unsorted List"+employees);
        Collections.sort(employees);
        System.out.println("Sorted by ID"+employees);
        employees.sort(compareByNameLength);
        System.out.println("Sorted by Name length"+employees);
    }
}
