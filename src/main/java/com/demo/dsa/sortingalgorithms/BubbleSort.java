package com.demo.dsa.sortingalgorithms;

public class BubbleSort {

    public static void main(String[] args) {
        int[] arrayOfNums = {6,5,2,8,9,4};
        int[] sortedArray = sortArrayUsingBubbleSort(arrayOfNums);

       for(int num : sortedArray){
           System.out.print(num+ " ");
       }
    }

    public static int[] sortArrayUsingBubbleSort(int[] arrayToSort){

        int lastIndex = arrayToSort.length-1;
        while(lastIndex >=1) {
            for(int num : arrayToSort){
                System.out.print(num+ " ");
            }
            System.out.println();

            for(int i = 0; i < lastIndex; i++){
                if(arrayToSort[i] > arrayToSort[i+1]){
                    int tempData = arrayToSort[i];
                    arrayToSort[i] = arrayToSort[i+1];
                    arrayToSort[i+1] = tempData;
                }
            }
            lastIndex--;
        }
        return arrayToSort;
    }

}
