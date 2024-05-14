package com.demo.threads;


public class ThreadImplementations {
    public void execute(){
        Runnable runnable = () -> System.out.println("Runnable Implementation!");
        runnable.run();

        A a  = (num1, num2) -> num1+num2;
        System.out.println(String.valueOf(a.addNumbers(5,6)));

        Thread thread1 = new Thread(new RunnableImplementation());
        thread1.start();

        Thread thread2 = new Thread(() -> {
            try{
                for (int i = 0; i < 10; i++) {
                    Thread.sleep(1000);
                    System.out.println("Running method " + i + "times from thread 2!");
                }
            } catch (Exception e){

            }
        });
        thread2.start();

        ClassWithOneAbstractMethod demo = new ClassWithOneAbstractMethod(){
            public int addNumbers(int a, int b){
                return a/b;
            }
        };
        System.out.println(demo.addNumbers(6, 12));
        }
    }

class ClassWithOneAbstractMethod implements A{
    public int addNumbers(int a, int b){
        return a*b;
    }

}

@FunctionalInterface
interface A{
    abstract int addNumbers(int a, int b);
}


class RunnableImplementation implements Runnable{
    @Override
    public void run() {
        for (int i = 0; i < 10; i++) {
            try{
                Thread.sleep(1000);
                System.out.println("Running method "+i+ "times from thread 1!");
            } catch (Exception e){

            }
        }
    }
}
