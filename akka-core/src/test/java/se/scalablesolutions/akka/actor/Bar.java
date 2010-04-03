package se.scalablesolutions.akka.actor;

import se.scalablesolutions.akka.actor.annotation.oneway;

public interface Bar {
  @oneway
  void bar(String msg);
  Ext getExt();
}
