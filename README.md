# Red Capped
a lightweight .NET message queue system built on top of MongoDb, with QoS support.

[![Build status](https://ci.appveyor.com/api/projects/status/34vnj5l5gdu6i3t4?svg=true)](https://ci.appveyor.com/project/petrhaus/redcapped)

### Define the message payload
```csharp
public class Order
{
  public int Id { get; set; }
  public decimal Amount { get; set; }
  ...
}
```

### How to publish messages 

```csharp
// create the queues manager
var manager = new RedCappedQueueManager("mongodb://localhost", "mydb");
// create the queue
var queue = await manager.CreateQueue<Order>(queueName, 256*1024*1024);
// publish!
await queue.PublishAsync("new-orders", new Order { Id = 123, Amount = 120M });
```
### How to subscribe and receive messages

```csharp
// create the queues manager
var manager = new RedCappedQueueManager("mongodb://localhost", "mydb");
// create the queue
var queue = await manager.CreateQueue<Order>(queueName, 256*1024*1024);
// subscribe the topic 'new-orders'
queue.Subscribe("new-orders", order =>
{
  Debug.WriteLine("Order #{0} amount {1}", order.Id, order.Amount);
  // if the message was handled, otherwise it will be requeued
  return true;
});
```
N.B. This is a work in progress and is subject to change, use it at your own risk!
