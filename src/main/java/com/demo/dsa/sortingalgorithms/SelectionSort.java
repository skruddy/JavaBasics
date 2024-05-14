package com.demo.dsa.sortingalgorithms;

public class SelectionSort {
    public static void main(String[] args) {
        int[] arrayOfNums = {6,5,2,8,9,4};
        int[] sortedArray = sortUsingSelectionSort(arrayOfNums);

        for(int num : sortedArray){
            System.out.print(num+ " ");
        }
    }

    public static  int[] sortUsingSelectionSort(int[] arrayToBeSorted){
        int lastIndex = arrayToBeSorted.length-1;

        while(lastIndex >= 1){
            int highestValue = -1;
            int indexOfHighestValue = lastIndex;
            for(int i = 0; i<= lastIndex; i++){
                if(arrayToBeSorted[i] > highestValue){
                    highestValue = arrayToBeSorted[i];
                    indexOfHighestValue = i;
                }
            }

            if(highestValue != -1){
                int tempData = arrayToBeSorted[indexOfHighestValue];
                arrayToBeSorted[indexOfHighestValue] = arrayToBeSorted[lastIndex];
                arrayToBeSorted[lastIndex] = tempData;
            }
            lastIndex--;
        }

        return arrayToBeSorted;
    }
}
