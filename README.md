一、需要考虑的问题:
a. 一个标志位,并发情况下只有一个线程可以获得此标志位
b. 释放锁失败,系统需要自释放锁

二、Redis VS Zookeeper VS DB
a. Redis提供超时机制,锁释放失败后,可以自动回收;Zookeeper的锁释放,只能在连接断开后自动回收,如果是程序异常导致锁释放代码未执行,则可能会引起线程无限等待,最终内存溢出或触发限流策略
b. zookeeper会为每个线程维护一个临时顺序节点,而redis只需要维护一个k-v对即可
c. db的unique key