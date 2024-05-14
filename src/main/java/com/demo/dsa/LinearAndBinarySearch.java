package com.demo.dsa;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class LinearAndBinarySearch {
    static int counterForRecursiveSearch = 0;

    public static void main(String[] args) {
        List<Integer> nums = IntStream.rangeClosed(0, 100000000)
                .boxed().collect(Collectors.toList());
        int target = 228;

        printOutput(linearSearch(nums, target));
        printOutput(binarySearchWithoutRecursive(nums, target));
        printOutput(binarySearchWithRecursive(nums, target, 0, nums.size()-1));


    }

    public static void printOutput(int targetIndex){
        if(targetIndex != -1)
            System.out.println("Element found!, Target index is : "+targetIndex);
        else
            System.out.println("Element not found! ");
    }

    public static int linearSearch(List<Integer> arrayOfInt, int target){
        int steps = 0;
        for(int i = 0; i<= arrayOfInt.size() -1; i++){
            steps++;
            if(arrayOfInt.get(i) == target){
                System.out.println("Steps taken for Linear Search: "+steps);
                return i;
            }
        }
        System.out.println("Steps taken for Linear Search: "+steps);
        return -1;
    }

    public static int binarySearchWithoutRecursive(List<Integer> arrayOfInt, int target){
        int steps = 0;
        int startIndex = 0;
        int endIndex = arrayOfInt.size() -1;

        while (startIndex != endIndex){
            steps++;
            int midIndex = (startIndex + endIndex)/2;

            if(arrayOfInt.get(midIndex)== target){
                System.out.println("Steps taken for Binary Search: "+steps);
                return midIndex;

            } else if (arrayOfInt.get(midIndex) < target) {
                startIndex = midIndex+1;

            } else if(arrayOfInt.get(midIndex) > target) {
                endIndex = midIndex - 1;
            }
        }

        System.out.println("Steps taken for Binary Search: "+steps);

        if(startIndex ==  endIndex && arrayOfInt.get(startIndex) == target){
            return startIndex;
        }

        return -1;
    }

    public static int binarySearchWithRecursive(List<Integer> arrayOfInt, int target, int startIndex, int endIndex){
        int targetIndex = -1;
        counterForRecursiveSearch++;

        if(startIndex <= endIndex){
            int midIndex = (startIndex + endIndex)/2;
            if(arrayOfInt.get(midIndex) == target){
                return midIndex;

            } else if (arrayOfInt.get(midIndex) < target) {
                targetIndex = binarySearchWithRecursive(arrayOfInt, target, midIndex+1, endIndex);

            } else if(arrayOfInt.get(midIndex) > target) {
                targetIndex = binarySearchWithRecursive(arrayOfInt, target, startIndex, midIndex - 1);
            }
        }

        System.out.println("Steps taken for Recursive Search: "+counterForRecursiveSearch);
        return targetIndex;
    }
}


