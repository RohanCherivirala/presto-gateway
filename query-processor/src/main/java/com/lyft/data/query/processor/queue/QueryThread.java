package com.lyft.data.query.processor.queue;

public class QueryThread extends Thread {
  Runnable routine;
  Runnable interrupted;

  public QueryThread(Runnable routine, Runnable interrupted) {
    this.routine = routine;
    this.interrupted = interrupted;
  }

  @Override
  public void run() {
    boolean completed = false;

    try {
      routine.run();
      completed = true;
    } finally {
      if (!completed) {
        interrupted.run();
      }
    }
  }
}
