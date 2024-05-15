package com.demo.dsa.sortingalgorithms;

public class InsertionSort {
    public static void main(String[] args) {
        int[] arrayOfNums = {6,5,2,8,8,9,4};
        int[] sortedArray = sortArrayByInsertionSort(arrayOfNums);

        for(int num : sortedArray){
            System.out.print(num+ " ");
        }
    }

    public static int[] sortArrayByInsertionSort(int[] arrayToBeSorted){
        int startIndex = 0;

        for(int i = startIndex; i< arrayToBeSorted.length-1; i++){
            if(arrayToBeSorted[i] > arrayToBeSorted[i+1]){
                int tempData = arrayToBeSorted[i+1];
                int indexToShift = i;

                for(int j = i; j >= 0; j--){
                    if(arrayToBeSorted[j] > tempData){
                        arrayToBeSorted[j+1] =  arrayToBeSorted[j];
                        indexToShift = j;
                    } else
                        break;
                }
                arrayToBeSorted[indexToShift] = tempData;
            }
        }

        return arrayToBeSorted;
    }

    public static int[] shiftElementsOfArray(int[] arrayToBeShifted, int index){


        return arrayToBeShifted;
    }
}
