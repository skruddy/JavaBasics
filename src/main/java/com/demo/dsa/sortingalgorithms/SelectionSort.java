package com.demo.dsa.sortingalgorithms;

public class SelectionSort {
    public static void main(String[] args) {
        int[] arrayOfNums = {6,5,2,8,8,9,4};
        int[] sortedArray = sortUsingSelectionSort(arrayOfNums);

        for(int num : sortedArray){
            System.out.print(num+ " ");
        }
    }

    public static  int[] sortUsingSelectionSort(int[] arrayToBeSorted){
        int lastIndex = arrayToBeSorted.length-1;

        while(lastIndex >= 1){
            int indexOfHighestValue = 0;
            for(int i = 0; i<= lastIndex; i++){
                if(arrayToBeSorted[i] > arrayToBeSorted[indexOfHighestValue]){
                    indexOfHighestValue = i;
                }
            }

            int tempData = arrayToBeSorted[indexOfHighestValue];
            arrayToBeSorted[indexOfHighestValue] = arrayToBeSorted[lastIndex];
            arrayToBeSorted[lastIndex] = tempData;

            lastIndex--;
        }

        return arrayToBeSorted;
    }
}
