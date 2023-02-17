/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.chunjun.cdc.cache;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LockedBlockingQueue<T> implements Queue<T>, Serializable {

    private static final long serialVersionUID = -5878734498497278866L;

    private final Lock lock = new ReentrantLock();

    private final BlockingQueue<T> queue;

    /** Timeout for reading or writing data. */
    private final long timeout;

    /** time-out unit */
    private final TimeUnit timeUnit = TimeUnit.MILLISECONDS;

    public LockedBlockingQueue(int size, long timeout) {
        this.queue = new LinkedBlockingQueue<>(size);
        this.timeout = timeout;
    }

    public int size() {
        lock.lock();
        try {
            return queue.size();
        } finally {
            lock.unlock();
        }
    }

    public T poll() {
        lock.lock();
        try {
            return queue.poll(timeout, timeUnit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            lock.unlock();
        }
        return null;
    }

    @Override
    public T element() {
        lock.lock();
        try {
            return queue.element();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public T peek() {
        lock.lock();
        try {
            return queue.peek();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean add(T t) {
        lock.lock();
        try {
            return queue.offer(t, timeout, timeUnit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            lock.unlock();
        }
        return false;
    }

    @Override
    public boolean remove(Object o) {
        lock.lock();
        try {
            return queue.remove(o);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        lock.lock();
        try {
            return queue.containsAll(c);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        if (null == c) {
            throw new NullPointerException();
        }
        lock.lock();
        try {
            return queue.addAll(c);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        if (null == c) {
            throw new NullPointerException();
        }
        lock.lock();
        try {
            return queue.removeAll(c);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        if (null == c) {
            throw new NullPointerException();
        }
        lock.lock();
        try {
            return queue.retainAll(c);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void clear() {
        lock.lock();
        try {
            queue.clear();
        } finally {
            lock.unlock();
        }
    }

    public boolean offer(T data) {
        lock.lock();
        try {
            return queue.offer(data, timeout, timeUnit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            lock.unlock();
        }
        return false;
    }

    @Override
    public T remove() {
        lock.lock();
        try {
            return queue.remove();
        } finally {
            lock.unlock();
        }
    }

    public Iterator<T> iterator() {
        lock.lock();
        try {
            return queue.iterator();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Object[] toArray() {
        lock.lock();
        try {
            return queue.toArray();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public <T1> T1[] toArray(T1[] a) {
        if (null == a) {
            throw new NullPointerException();
        }
        lock.lock();
        try {
            return queue.toArray(a);
        } finally {
            lock.unlock();
        }
    }

    public boolean isEmpty() {
        lock.lock();
        try {
            return queue.isEmpty();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean contains(Object o) {
        lock.lock();
        try {
            return queue.contains(o);
        } finally {
            lock.unlock();
        }
    }

    public Queue<T> getAll() {
        return queue;
    }
}
