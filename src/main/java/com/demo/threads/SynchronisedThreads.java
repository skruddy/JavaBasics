package com.demo.threads;

class Counter{
    int counter = 0;
    public synchronized void increment(){
        counter++;
    }
}

public class SynchronisedThreads {
    public static void main(String[] args) throws InterruptedException {
        Counter c = new Counter();


        //Creating thread via passing runnable implementation as an argument
        Thread thread1 = new Thread(() -> {
            for (int i = 0; i < 1000; i++) {
                c.increment();
            }
        });

        //Creating thread via anonymous class implementation
        Thread thread2 = new Thread(){
            public void run(){
                for (int i = 0; i < 1000; i++) {
                    c.increment();
                }
            }
        };

        //Creating thread via implementing runnable as a lambda
        Runnable runnableImplementation = () -> {
            for (int i = 0; i < 1000; i++) {
                c.increment();
            }
        };
        Thread thread3 = new Thread(runnableImplementation);

        thread1.start();
        thread2.start();
        thread3.start();

        thread1.join();
        thread2.join();
        thread3.join();


        System.out.println(c.counter);
    }

}


