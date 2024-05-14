package com.demo.collections;

import com.demo.model.Employee;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class StreamDemo {

    public static void main(String[] args) {
        List<Integer> nums = Arrays.asList(2,4,6,8,10, 11,16,76, 4,5,7,1,2,4546,36,253,2,1,13,24);
        List<Employee> employees = new ArrayList<>();
        employees.add(new Employee(4, "Saaquib", "Khan"));
        employees.add(new Employee(2, "Shekhu", ""));
        employees.add(new Employee(1, "Shaktiimaannn", ""));
        employees.add(new Employee(6, "SkRuddy", ""));
        employees.add(new Employee(9, "", ""));


        Optional<Integer> biggestNumber = nums.stream().reduce((num1, num2) -> num1>num2 ? num1 : num2);
        Optional<Employee> employeeWithBiggestName = employees.stream().reduce((employee1, employee2) -> {
            int emp1NameLength = employee1.getName().length();
            int emp2NameLength = employee2.getName().length();
            if(emp2NameLength > emp1NameLength)
                return employee2;
            else
                return employee1;
        });
        List<String> strings = nums.stream().map(num -> num.toString()+"num").collect(Collectors.toList());
        System.out.println(strings);
        System.out.println(biggestNumber);
        System.out.println(employeeWithBiggestName);
    }
}
