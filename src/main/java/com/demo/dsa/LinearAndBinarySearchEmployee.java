package com.demo.dsa;

import com.demo.model.Employee;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class LinearAndBinarySearchEmployee {
    static int counterForRecursiveSearch = 0;
    private static Random random = new Random();

    public static void main(String[] args) {

        List<Employee> employees = getEmployeesList(10000000);
        int target = 10000;//random.nextInt(1, 10000000);

        printOutput(linearSearchEmployee(employees, target));
        printOutput(binarySearchWithoutRecursiveEmployee(employees, target));

        long startTime = System.currentTimeMillis();
        printOutput(binarySearchWithRecursiveEmployee(employees, target, 0, employees.size()-1));
        long timeTaken = System.currentTimeMillis() - startTime;
        System.out.println("Time taken for Recursive Search of Employees : "+timeTaken + "ms");


    }

    public static List<Employee> getEmployeesList(int employeeCapacity){
        Set<Employee> employees = new HashSet<>(employeeCapacity);

        for(int i = 0; i < employeeCapacity; i++){
            employees.add(new Employee(random.nextInt(1, employeeCapacity), "Test Name", "test.id@test.com"));
        }
        List<Employee> sortedEmployees = new ArrayList<>(employees);
        Collections.sort(sortedEmployees);
        return sortedEmployees;
    }

    public static void printOutput(int targetIndex){
        if(targetIndex != -1)
            System.out.println("Element found!, Target index is : "+targetIndex);
        else
            System.out.println("Element not found! ");
    }

    public static int linearSearchEmployee(List<Employee> employees, int targetId){
        long startTime = System.currentTimeMillis();
        for(int i = 0; i<= employees.size() -1; i++){
            if(employees.get(i).getId() == targetId){
                long timeTaken = System.currentTimeMillis() - startTime;
                System.out.println("Time taken for Linear Search of Employees : "+timeTaken + "ms");
                return i;
            }
        }
        long timeTaken = System.currentTimeMillis() - startTime;
        System.out.println("Time taken for Linear Search of Employees : "+timeTaken + "ms");
        return -1;
    }

    public static int binarySearchWithoutRecursiveEmployee(List<Employee> employees, int target){
        long startTime = System.currentTimeMillis();
        int startIndex = 0;
        int endIndex = employees.size() -1;

        while (startIndex != endIndex){
            int midIndex = (startIndex + endIndex)/2;

            if(employees.get(midIndex).getId() == target){
                long timeTaken = System.currentTimeMillis() - startTime;
                System.out.println("Time taken for Binary Search of Employees : "+timeTaken + "ms");
                return midIndex;

            } else if (employees.get(midIndex).getId() < target) {
                startIndex = midIndex+1;

            } else if(employees.get(midIndex).getId() > target) {
                endIndex = midIndex - 1;
            }
        }

        if((startIndex ==  endIndex) && employees.get(startIndex).getId() == target){
            long timeTaken = System.currentTimeMillis() - startTime;
            System.out.println("Time taken for Binary Search of Employees : "+timeTaken + "ms");
            return startIndex;
        }

        long timeTaken = System.currentTimeMillis() - startTime;
        System.out.println("Time taken for Binary Search of Employees : "+timeTaken + "ms");
        return -1;
    }

    public static int binarySearchWithRecursiveEmployee(List<Employee> employees, int target, int startIndex, int endIndex){

        int targetIndex = -1;
        counterForRecursiveSearch++;

        if(startIndex < endIndex){
            int midIndex = (startIndex + endIndex)/2;
            if(employees.get(midIndex).getId() == target){
                return midIndex;

            } else if (employees.get(midIndex).getId() < target) {
                targetIndex = binarySearchWithRecursiveEmployee(employees, target, midIndex+1, endIndex);

            } else if(employees.get(midIndex).getId() > target) {
                targetIndex = binarySearchWithRecursiveEmployee(employees, target, startIndex, midIndex - 1);
            }
        }


        return targetIndex;
    }
}


